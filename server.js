/**
 * MIDNIGHT GROUP CHAT — Backend
 * Express + Socket.IO, fully in-memory (no database).
 * Deploy target: https://midnight-back.onrender.com
 */

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const crypto = require('crypto');

const uuid = () => crypto.randomUUID();

const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '30mb' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 30 * 1024 * 1024, // allow base64 media/voice notes over the socket
});

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'changeme';
const MAX_MESSAGES = 800;

// ---------------------------------------------------------------------------
// In-memory state
// ---------------------------------------------------------------------------
const group = {
  name: 'Midnight Group Chat',
  description: 'Welcome to the group. Be nice. 🌙',
  picture: null, // base64 data url or null
  locked: false, // when true, only admins can send messages
  createdAt: Date.now(),
};

/** userId -> user object */
const users = {};
/** userId -> user object (subset of users where status === 'pending') */
const pendingRequests = {};
/** array of message objects, newest last */
const messages = [];
/** valid admin session tokens (from admin.html password login) */
const adminTokens = new Set();

function publicUser(u) {
  return {
    id: u.id,
    username: u.username,
    isAdmin: !!u.isAdmin,
    online: !!u.online,
    avatar: u.avatar || null,
    status: u.status,
    joinedAt: u.joinedAt,
    lastSeen: u.lastSeen,
  };
}

function approvedMembersPublic() {
  return Object.values(users)
    .filter((u) => u.status === 'approved')
    .map(publicUser);
}

function pendingPublic() {
  return Object.values(pendingRequests).map(publicUser);
}

function broadcastMembers() {
  io.emit('members_update', approvedMembersPublic());
}

function broadcastPending() {
  io.to('admins').emit('pending_update', pendingPublic());
}

function usernameTaken(name) {
  return Object.values(users).some(
    (u) => u.username.toLowerCase() === name.toLowerCase() && u.status !== 'rejected'
  );
}

function pushMessage(msg) {
  messages.push(msg);
  if (messages.length > MAX_MESSAGES) messages.shift();
}

// ---------------------------------------------------------------------------
// HTTP routes
// ---------------------------------------------------------------------------
app.get('/health', (req, res) => res.json({ ok: true, name: group.name }));

app.post('/admin/login', (req, res) => {
  const { password } = req.body || {};
  if (typeof password === 'string' && password.length && password === ADMIN_PASSWORD) {
    const token = uuid();
    adminTokens.add(token);
    return res.json({ success: true, token });
  }
  return res.status(401).json({ success: false, message: 'Invalid password.' });
});

app.post('/admin/logout', (req, res) => {
  const { token } = req.body || {};
  adminTokens.delete(token);
  res.json({ success: true });
});

// ---------------------------------------------------------------------------
// Socket.IO
// ---------------------------------------------------------------------------
io.on('connection', (socket) => {
  let currentUser = null;

  function requireAdmin(token) {
    if (token && adminTokens.has(token)) return true;
    return !!(currentUser && currentUser.isAdmin);
  }

  // ---- Joining ----
  socket.on('join_request', ({ username }) => {
    username = String(username || '').trim().slice(0, 24);
    if (!username) return socket.emit('join_error', 'Please enter a username.');
    if (usernameTaken(username)) {
      return socket.emit('join_error', 'That username is taken or already pending approval.');
    }
    const id = uuid();
    const user = {
      id,
      username,
      socketId: socket.id,
      isAdmin: false,
      status: 'pending',
      online: false,
      avatar: null,
      joinedAt: Date.now(),
      lastSeen: Date.now(),
    };
    users[id] = user;
    pendingRequests[id] = user;
    currentUser = user;
    socket.data.userId = id;
    socket.emit('join_pending', publicUser(user));
    broadcastPending();
  });

  // Reconnect using a userId saved in the client's localStorage
  socket.on('reconnect_user', ({ userId }) => {
    const u = users[userId];
    if (!u) return socket.emit('force_rejoin');
    currentUser = u;
    socket.data.userId = userId;
    u.socketId = socket.id;
    if (u.status === 'approved') {
      u.online = true;
      u.lastSeen = Date.now();
      socket.emit('reconnected', publicUser(u));
      socket.emit('chat_history', messages);
      socket.emit('group_update', group);
      socket.emit('members_update', approvedMembersPublic());
      broadcastMembers();
    } else if (u.status === 'pending') {
      socket.emit('join_pending', publicUser(u));
    } else {
      socket.emit('force_rejoin');
    }
  });

  // ---- Admin auth (from admin.html) ----
  socket.on('admin_auth', ({ token }) => {
    if (adminTokens.has(token)) {
      socket.join('admins');
      socket.data.isAdminPanel = true;
      socket.emit('admin_auth_ok');
      socket.emit('pending_update', pendingPublic());
      socket.emit('members_update', approvedMembersPublic());
      socket.emit('group_update', group);
    } else {
      socket.emit('admin_auth_fail');
    }
  });

  socket.on('admin_approve', ({ token, userId }) => {
    if (!requireAdmin(token)) return;
    const u = users[userId];
    if (!u) return;
    u.status = 'approved';
    delete pendingRequests[userId];
    broadcastPending();
    broadcastMembers();
    io.to(u.socketId).emit('approved', publicUser(u));
  });

  socket.on('admin_reject', ({ token, userId }) => {
    if (!requireAdmin(token)) return;
    const u = users[userId];
    if (!u) return;
    io.to(u.socketId).emit('rejected');
    delete pendingRequests[userId];
    delete users[userId];
    broadcastPending();
  });

  socket.on('admin_promote', ({ token, userId }) => {
    if (!requireAdmin(token)) return;
    const u = users[userId];
    if (u) {
      u.isAdmin = true;
      broadcastMembers();
      if (u.socketId) io.to(u.socketId).emit('you_are_admin', true);
    }
  });

  socket.on('admin_demote', ({ token, userId }) => {
    if (!requireAdmin(token)) return;
    const u = users[userId];
    if (u) {
      u.isAdmin = false;
      broadcastMembers();
      if (u.socketId) io.to(u.socketId).emit('you_are_admin', false);
    }
  });

  socket.on('admin_kick', ({ token, userId }) => {
    if (!requireAdmin(token)) return;
    const u = users[userId];
    if (u) {
      if (u.socketId) io.to(u.socketId).emit('kicked');
      delete users[userId];
      broadcastMembers();
    }
  });

  // ---- Messaging ----
  socket.on('send_message', (payload = {}) => {
    const u = currentUser;
    if (!u || u.status !== 'approved') return;
    if (group.locked && !u.isAdmin) {
      return socket.emit('error_message', 'The chat is locked. Only admins can send messages right now.');
    }
    const type = ['text', 'image', 'voice', 'sticker', 'view_once'].includes(payload.type)
      ? payload.type
      : 'text';
    if (type === 'text' && !String(payload.content || '').trim()) return;

    const msg = {
      id: uuid(),
      type,
      content: payload.content, // text string OR base64 data url for media
      caption: payload.caption ? String(payload.caption).slice(0, 500) : null,
      from: u.id,
      fromUsername: u.username,
      replyTo: payload.replyTo || null, // { id, fromUsername, preview }
      mentions: Array.isArray(payload.mentions) ? payload.mentions.slice(0, 20) : [],
      reactions: {}, // emoji -> [userId]
      pinned: false,
      viewOnce: type === 'view_once',
      viewed: false,
      deleted: false,
      timestamp: Date.now(),
    };
    pushMessage(msg);
    io.emit('new_message', msg);
  });

  socket.on('react_message', ({ messageId, emoji }) => {
    const u = currentUser;
    if (!u || !emoji) return;
    const msg = messages.find((m) => m.id === messageId);
    if (!msg) return;
    if (!msg.reactions[emoji]) msg.reactions[emoji] = [];
    const idx = msg.reactions[emoji].indexOf(u.id);
    if (idx >= 0) msg.reactions[emoji].splice(idx, 1);
    else msg.reactions[emoji].push(u.id);
    if (msg.reactions[emoji].length === 0) delete msg.reactions[emoji];
    io.emit('message_updated', msg);
  });

  socket.on('view_once_open', ({ messageId }) => {
    const u = currentUser;
    const msg = messages.find((m) => m.id === messageId);
    if (!msg || !u || msg.type !== 'view_once') return;
    if (msg.from !== u.id && !msg.viewed) {
      msg.viewed = true;
      msg.content = null; // burn the media after the recipient viewed it once
      io.emit('message_updated', msg);
    }
  });

  socket.on('pin_message', ({ messageId }) => {
    const u = currentUser;
    if (!u || !u.isAdmin) return;
    const msg = messages.find((m) => m.id === messageId);
    if (msg) {
      msg.pinned = true;
      io.emit('message_updated', msg);
    }
  });

  socket.on('unpin_message', ({ messageId }) => {
    const u = currentUser;
    if (!u || !u.isAdmin) return;
    const msg = messages.find((m) => m.id === messageId);
    if (msg) {
      msg.pinned = false;
      io.emit('message_updated', msg);
    }
  });

  socket.on('delete_message', ({ messageId }) => {
    const u = currentUser;
    const msg = messages.find((m) => m.id === messageId);
    if (!msg || !u) return;
    if (msg.from !== u.id && !u.isAdmin) return;
    msg.deleted = true;
    msg.content = null;
    msg.caption = null;
    msg.pinned = false;
    io.emit('message_updated', msg);
  });

  socket.on('typing', ({ isTyping }) => {
    const u = currentUser;
    if (!u || u.status !== 'approved') return;
    socket.broadcast.emit('user_typing', { userId: u.id, username: u.username, isTyping: !!isTyping });
  });

  // ---- Group settings (admin only) ----
  socket.on('update_group', ({ token, name, description, picture, locked }) => {
    if (!requireAdmin(token)) return;
    if (typeof name === 'string' && name.trim()) group.name = name.trim().slice(0, 60);
    if (typeof description === 'string') group.description = description.slice(0, 300);
    if (typeof picture === 'string' || picture === null) group.picture = picture;
    if (typeof locked === 'boolean') group.locked = locked;
    io.emit('group_update', group);
  });

  // ---- Disconnect ----
  socket.on('disconnect', () => {
    const u = currentUser;
    if (!u) return;
    if (u.status === 'approved') {
      u.online = false;
      u.lastSeen = Date.now();
      broadcastMembers();
    } else if (u.status === 'pending') {
      delete pendingRequests[u.id];
      delete users[u.id];
      broadcastPending();
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Midnight Group Chat backend listening on port ${PORT}`);
  console.log(`Admin password is ${ADMIN_PASSWORD === 'changeme' ? 'DEFAULT (changeme) — set ADMIN_PASSWORD env var!' : 'configured via env var.'}`);
});
