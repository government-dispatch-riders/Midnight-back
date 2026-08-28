/**
 * MIDNIGHT GROUP CHAT — Backend
 * Express + Socket.IO + Supabase persistence.
 */

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const uuid = () => crypto.randomUUID();
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '35mb' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 35 * 1024 * 1024,
});

const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'changeme';
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MAX_MESSAGES = 800;
const MAX_ANNOUNCEMENTS = 100;
const MEDIA_BUCKET = process.env.SUPABASE_MEDIA_BUCKET || 'midnight-media';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. Persistence cannot work.');
}
const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

const group = {
  id: 1,
  name: 'Midnight Group Chat',
  description: 'Welcome to the group. Be nice. 🌙',
  picture: null,
  locked: false,
  createdAt: Date.now(),
};
const users = {};
const pendingRequests = {};
const messages = [];
const adminTokens = new Set();
let ready = false;

function publicUser(u) {
  return {
    id: u.id, username: u.username, isAdmin: !!u.isAdmin, online: !!u.online,
    avatar: u.avatar || null, status: u.status, joinedAt: u.joinedAt, lastSeen: u.lastSeen,
  };
}
function approvedMembersPublic() { return Object.values(users).filter(u => u.status === 'approved').map(publicUser); }
function pendingPublic() { return Object.values(pendingRequests).map(publicUser); }
function broadcastMembers() { io.emit('members_update', approvedMembersPublic()); }
function broadcastPending() { io.to('admins').emit('pending_update', pendingPublic()); }
function usernameTaken(name) { return Object.values(users).some(u => u.username.toLowerCase() === name.toLowerCase() && u.status !== 'rejected'); }
function pushMessage(msg) { messages.push(msg); if (messages.length > MAX_MESSAGES) messages.shift(); }
function dbUser(u) {
  return { id: u.id, username: u.username, is_admin: !!u.isAdmin, status: u.status, avatar: u.avatar || null, joined_at: new Date(u.joinedAt).toISOString(), last_seen: new Date(u.lastSeen).toISOString() };
}
function dbMessage(m) {
  return { id: m.id, type: m.type, content: m.content, caption: m.caption || null, from_user_id: m.from, from_username: m.fromUsername,
    reply_to: m.replyTo || null, mentions: m.mentions || [], reactions: m.reactions || {}, pinned: !!m.pinned, view_once: !!m.viewOnce,
    viewed: !!m.viewed, deleted: !!m.deleted, timestamp: new Date(m.timestamp).toISOString() };
}
function fromDbMessage(r) {
  return { id: r.id, type: r.type, content: r.content, caption: r.caption, from: r.from_user_id, fromUsername: r.from_username,
    replyTo: r.reply_to, mentions: r.mentions || [], reactions: r.reactions || {}, pinned: !!r.pinned, viewOnce: !!r.view_once,
    viewed: !!r.viewed, deleted: !!r.deleted, timestamp: new Date(r.timestamp).getTime() };
}

async function dbUpdateUser(u) {
  if (!supabase) return;
  const { error } = await supabase.from('users').upsert(dbUser(u));
  if (error) console.error('users upsert:', error.message);
}
async function dbUpdateMessage(m) {
  if (!supabase) return;
  const { error } = await supabase.from('messages').upsert(dbMessage(m));
  if (error) console.error('messages upsert:', error.message);
}
async function dbUpdateGroup() {
  if (!supabase) return;
  const { error } = await supabase.from('group_settings').upsert({ id: 1, name: group.name, description: group.description, picture: group.picture, locked: group.locked, created_at: new Date(group.createdAt).toISOString() });
  if (error) console.error('group_settings upsert:', error.message);
}

function parseDataUrl(dataUrl) {
  const match = /^data:([^;]+);base64,(.+)$/s.exec(String(dataUrl || ''));
  if (!match) return null;
  return { contentType: match[1], buffer: Buffer.from(match[2], 'base64') };
}
function extensionFor(contentType) {
  const map = { 'image/jpeg':'jpg','image/png':'png','image/webp':'webp','image/gif':'gif','video/mp4':'mp4','video/webm':'webm','audio/webm':'webm','audio/mpeg':'mp3' };
  return map[contentType] || 'bin';
}
async function uploadDataUrl(dataUrl, folder = 'chat') {
  if (!supabase) return dataUrl;
  const parsed = parseDataUrl(dataUrl);
  if (!parsed) return dataUrl;
  const ext = extensionFor(parsed.contentType);
  const path = `${folder}/${Date.now()}-${uuid()}.${ext}`;
  const { error } = await supabase.storage.from(MEDIA_BUCKET).upload(path, parsed.buffer, { contentType: parsed.contentType, upsert: false });
  if (error) throw new Error(`Media upload failed: ${error.message}`);
  const { data } = supabase.storage.from(MEDIA_BUCKET).getPublicUrl(path);
  return data.publicUrl;
}

async function ensureStorageBucket() {
  if (!supabase) return;
  const { data } = await supabase.storage.getBucket(MEDIA_BUCKET);
  if (data) return;
  const { error } = await supabase.storage.createBucket(MEDIA_BUCKET, { public: true, fileSizeLimit: '35MB' });
  if (error && !/already exists/i.test(error.message)) console.error('Storage bucket:', error.message);
}

async function loadState() {
  if (!supabase) return;
  try {
    await ensureStorageBucket();
    const [gRes, uRes, mRes] = await Promise.all([
      supabase.from('group_settings').select('*').eq('id', 1).maybeSingle(),
      supabase.from('users').select('*'),
      supabase.from('messages').select('*').order('timestamp', { ascending: true }).limit(MAX_MESSAGES),
    ]);
    if (gRes.error) throw gRes.error;
    if (uRes.error) throw uRes.error;
    if (mRes.error) throw mRes.error;
    if (gRes.data) {
      group.name = gRes.data.name || group.name;
      group.description = gRes.data.description || '';
      group.picture = gRes.data.picture || null;
      group.locked = !!gRes.data.locked;
      group.createdAt = new Date(gRes.data.created_at).getTime() || group.createdAt;
    } else await dbUpdateGroup();
    Object.keys(users).forEach(k => delete users[k]);
    Object.keys(pendingRequests).forEach(k => delete pendingRequests[k]);
    (uRes.data || []).forEach(r => {
      const u = { id:r.id, username:r.username, isAdmin:!!r.is_admin, status:r.status, avatar:r.avatar, online:false,
        socketId:null, joinedAt:new Date(r.joined_at).getTime(), lastSeen:new Date(r.last_seen).getTime() };
      users[u.id] = u;
      if (u.status === 'pending') pendingRequests[u.id] = u;
    });
    messages.splice(0, messages.length, ...(mRes.data || []).map(fromDbMessage));
    ready = true;
    console.log(`Supabase loaded: ${Object.keys(users).length} users, ${messages.length} messages.`);
  } catch (err) {
    console.error('Supabase startup load failed:', err.message || err);
    process.exit(1);
  }
}

async function getAnnouncementsForUser(userId) {
  if (!supabase) return { items: [], unseenCount: 0 };
  const [aRes, rRes] = await Promise.all([
    supabase.from('announcements').select('*').order('created_at', { ascending: false }).limit(MAX_ANNOUNCEMENTS),
    supabase.from('announcement_reads').select('announcement_id').eq('user_id', userId),
  ]);
  if (aRes.error) throw aRes.error;
  if (rRes.error) throw rRes.error;
  const readSet = new Set((rRes.data || []).map(x => x.announcement_id));
  const items = (aRes.data || []).map(a => ({ id:a.id, link:a.link || null, mediaUrl:a.media_url || null, mediaType:a.media_type || null, caption:a.caption || '', createdAt:new Date(a.created_at).getTime(), seen:readSet.has(a.id) }));
  return { items, unseenCount: items.filter(x => !x.seen).length };
}
async function broadcastAnnouncements() {
  for (const u of Object.values(users).filter(x => x.status === 'approved' && x.socketId)) {
    try { io.to(u.socketId).emit('announcements_update', await getAnnouncementsForUser(u.id)); } catch (e) { console.error('announcement broadcast:', e.message); }
  }
}
async function createAnnouncement({ link, caption, mediaData, mediaType }) {
  link = String(link || '').trim().slice(0, 1000);
  caption = String(caption || '').slice(0, 500);
  if (link && !/^https?:\/\//i.test(link)) throw new Error('Announcement links must start with http:// or https://.');
  if (!link && !mediaData) throw new Error('Add a link, photo, or video before publishing.');
  let mediaUrl = null;
  if (mediaData) mediaUrl = await uploadDataUrl(mediaData, 'announcements');
  const { data, error } = await supabase.from('announcements').insert({ link: link || null, caption: String(caption || '').slice(0, 500), media_url: mediaUrl, media_type: mediaType || null }).select('*').single();
  if (error) throw error;
  return data;
}

// HTTP routes
app.get('/health', (req, res) => res.json({ ok: true, name: group.name, persistence: !!supabase, ready }));
app.post('/admin/login', (req, res) => {
  const { password } = req.body || {};
  if (typeof password === 'string' && password.length && password === ADMIN_PASSWORD) {
    const token = uuid(); adminTokens.add(token); return res.json({ success:true, token });
  }
  return res.status(401).json({ success:false, message:'Invalid password.' });
});
app.post('/admin/logout', (req, res) => { adminTokens.delete(req.body?.token); res.json({ success:true }); });

io.on('connection', (socket) => {
  let currentUser = null;
  function requireAdmin(token) { return !!(token && adminTokens.has(token)) || !!(currentUser && currentUser.isAdmin); }
  socket.on('join_request', async ({ username }) => {
    username = String(username || '').trim().slice(0, 24);
    if (!username) return socket.emit('join_error', 'Please enter a username.');
    if (usernameTaken(username)) return socket.emit('join_error', 'That username is taken or already pending approval.');
    const now = Date.now();
    const u = { id:uuid(), username, socketId:socket.id, isAdmin:false, status:'pending', online:false, avatar:null, joinedAt:now, lastSeen:now };
    users[u.id] = u; pendingRequests[u.id] = u; currentUser = u; socket.data.userId = u.id;
    await dbUpdateUser(u);
    socket.emit('join_pending', publicUser(u)); broadcastPending();
  });

  socket.on('reconnect_user', async ({ userId }) => {
    const u = users[userId];
    if (!u) return socket.emit('force_rejoin');
    currentUser = u; socket.data.userId = userId; u.socketId = socket.id;
    if (u.status === 'approved') {
      u.online = true; u.lastSeen = Date.now(); await dbUpdateUser(u);
      socket.emit('reconnected', publicUser(u)); socket.emit('chat_history', messages); socket.emit('group_update', group);
      socket.emit('members_update', approvedMembersPublic()); socket.emit('announcements_update', await getAnnouncementsForUser(u.id)); broadcastMembers();
    } else if (u.status === 'pending') socket.emit('join_pending', publicUser(u));
    else socket.emit('force_rejoin');
  });

  socket.on('admin_auth', ({ token }) => {
    if (!adminTokens.has(token)) return socket.emit('admin_auth_fail');
    socket.join('admins'); socket.data.isAdminPanel = true; socket.emit('admin_auth_ok');
    socket.emit('pending_update', pendingPublic()); socket.emit('members_update', approvedMembersPublic()); socket.emit('group_update', group);
    getAnnouncementsForUser('00000000-0000-0000-0000-000000000000').then(({items}) => socket.emit('admin_announcements', items)).catch(()=>socket.emit('admin_announcements', []));
  });

  socket.on('admin_approve', async ({ token, userId }) => { if(!requireAdmin(token)) return; const u=users[userId]; if(!u)return; u.status='approved'; delete pendingRequests[userId]; await dbUpdateUser(u); broadcastPending(); broadcastMembers(); if(u.socketId) io.to(u.socketId).emit('approved', publicUser(u)); });
  socket.on('admin_reject', async ({ token, userId }) => { if(!requireAdmin(token))return; const u=users[userId]; if(!u)return; if(u.socketId)io.to(u.socketId).emit('rejected'); delete pendingRequests[userId]; delete users[userId]; if(supabase) await supabase.from('users').delete().eq('id', userId); broadcastPending(); });
  socket.on('admin_promote', async ({ token, userId }) => { if(!requireAdmin(token))return; const u=users[userId]; if(u){u.isAdmin=true; await dbUpdateUser(u); broadcastMembers(); if(u.socketId)io.to(u.socketId).emit('you_are_admin',true);} });
  socket.on('admin_demote', async ({ token, userId }) => { if(!requireAdmin(token))return; const u=users[userId]; if(u){u.isAdmin=false; await dbUpdateUser(u); broadcastMembers(); if(u.socketId)io.to(u.socketId).emit('you_are_admin',false);} });
  socket.on('admin_kick', async ({ token, userId }) => { if(!requireAdmin(token))return; const u=users[userId]; if(u){if(u.socketId)io.to(u.socketId).emit('kicked'); delete users[userId]; if(supabase)await supabase.from('users').delete().eq('id',userId); broadcastMembers();} });

  socket.on('admin_create_announcement', async ({ token, link, caption, mediaData, mediaType }) => {
    if(!requireAdmin(token) || !supabase) return;
    try { await createAnnouncement({link, caption, mediaData, mediaType}); socket.emit('admin_announcement_saved'); await broadcastAnnouncements(); const result = await supabase.from('announcements').select('*').order('created_at',{ascending:false}).limit(MAX_ANNOUNCEMENTS); socket.emit('admin_announcements',(result.data||[]).map(a=>({id:a.id,link:a.link||null,mediaUrl:a.media_url||null,mediaType:a.media_type||null,caption:a.caption||'',createdAt:new Date(a.created_at).getTime()}))); }
    catch(e){ socket.emit('admin_announcement_error', e.message || 'Could not save announcement.'); }
  });
  socket.on('admin_delete_announcement', async ({ token, announcementId }) => {
    if(!requireAdmin(token) || !supabase) return;
    const { error } = await supabase.from('announcements').delete().eq('id', announcementId);
    if(error) return socket.emit('admin_announcement_error', error.message);
    await broadcastAnnouncements();
    const result = await supabase.from('announcements').select('*').order('created_at',{ascending:false}).limit(MAX_ANNOUNCEMENTS);
    socket.emit('admin_announcements',(result.data||[]).map(a=>({id:a.id,link:a.link||null,mediaUrl:a.media_url||null,mediaType:a.media_type||null,caption:a.caption||'',createdAt:new Date(a.created_at).getTime()})));
  });

  socket.on('mark_announcement_read', async ({ announcementId }) => {
    const u=currentUser; if(!u || u.status!=='approved' || !supabase || !announcementId)return;
    const { error } = await supabase.from('announcement_reads').upsert({announcement_id:announcementId,user_id:u.id,read_at:new Date().toISOString()},{onConflict:'announcement_id,user_id'});
    if(!error) socket.emit('announcements_update', await getAnnouncementsForUser(u.id));
  });

  socket.on('send_message', async (payload={}) => {
    const u=currentUser; if(!u || u.status!=='approved')return;
    if(group.locked && !u.isAdmin)return socket.emit('error_message','The chat is locked. Only admins can send messages right now.');
    const type=['text','image','voice','sticker','view_once'].includes(payload.type)?payload.type:'text';
    if(type==='text' && !String(payload.content||'').trim())return;
    try {
      let content=payload.content;
      if(['image','voice','sticker','view_once'].includes(type) && String(content||'').startsWith('data:')) content=await uploadDataUrl(content,'chat');
      const msg={id:uuid(),type,content,caption:payload.caption?String(payload.caption).slice(0,500):null,from:u.id,fromUsername:u.username,replyTo:payload.replyTo||null,mentions:Array.isArray(payload.mentions)?payload.mentions.slice(0,20):[],reactions:{},pinned:false,viewOnce:type==='view_once',viewed:false,deleted:false,timestamp:Date.now()};
      pushMessage(msg); await dbUpdateMessage(msg); io.emit('new_message',msg);
    } catch(e) { socket.emit('error_message', e.message || 'Could not send media.'); }
  });

  socket.on('react_message', async ({messageId,emoji})=>{const u=currentUser;if(!u||!emoji)return;const m=messages.find(x=>x.id===messageId);if(!m)return;if(!m.reactions[emoji])m.reactions[emoji]=[];const i=m.reactions[emoji].indexOf(u.id);if(i>=0)m.reactions[emoji].splice(i,1);else m.reactions[emoji].push(u.id);if(!m.reactions[emoji].length)delete m.reactions[emoji];await dbUpdateMessage(m);io.emit('message_updated',m);});
  socket.on('view_once_open', async ({messageId})=>{const u=currentUser,m=messages.find(x=>x.id===messageId);if(!m||!u||m.type!=='view_once')return;if(m.from!==u.id&&!m.viewed){m.viewed=true;m.content=null;await dbUpdateMessage(m);io.emit('message_updated',m);}});
  socket.on('pin_message', async ({messageId})=>{const u=currentUser;if(!u||!u.isAdmin)return;const m=messages.find(x=>x.id===messageId);if(m){m.pinned=true;await dbUpdateMessage(m);io.emit('message_updated',m);}});
  socket.on('unpin_message', async ({messageId})=>{const u=currentUser;if(!u||!u.isAdmin)return;const m=messages.find(x=>x.id===messageId);if(m){m.pinned=false;await dbUpdateMessage(m);io.emit('message_updated',m);}});
  socket.on('delete_message', async ({messageId})=>{const u=currentUser,m=messages.find(x=>x.id===messageId);if(!m||!u)return;if(m.from!==u.id&&!u.isAdmin)return;m.deleted=true;m.content=null;m.caption=null;m.pinned=false;await dbUpdateMessage(m);io.emit('message_updated',m);});
  socket.on('typing', ({isTyping})=>{const u=currentUser;if(!u||u.status!=='approved')return;socket.broadcast.emit('user_typing',{userId:u.id,username:u.username,isTyping:!!isTyping});});
  socket.on('update_group', async ({token,name,description,picture,locked})=>{if(!requireAdmin(token))return;if(typeof name==='string'&&name.trim())group.name=name.trim().slice(0,60);if(typeof description==='string')group.description=description.slice(0,300);if(typeof picture==='string'||picture===null)group.picture=picture;if(typeof locked==='boolean')group.locked=locked;await dbUpdateGroup();io.emit('group_update',group);});

  socket.on('disconnect', async ()=>{const u=currentUser;if(!u)return;if(u.status==='approved'){u.online=false;u.lastSeen=Date.now();u.socketId=null;await dbUpdateUser(u);broadcastMembers();} /* pending users remain pending across Render restarts/disconnects */ });
});

const PORT=process.env.PORT||3000;
(async()=>{await loadState();server.listen(PORT,()=>{console.log(`Midnight Group Chat backend listening on port ${PORT}`);console.log(`Supabase persistence: ${!!supabase ? 'ENABLED':'DISABLED'}`);console.log(`Admin password: ${ADMIN_PASSWORD==='changeme'?'DEFAULT — set ADMIN_PASSWORD in Render':'configured'}`);});})();
