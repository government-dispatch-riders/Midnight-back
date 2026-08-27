const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const crypto=require('crypto');
const path=require('path');
const app=express(), server=http.createServer(app), io=new Server(server,{maxHttpBufferSize:8e6});
app.use(express.json({limit:'9mb'})); app.use(express.static(path.join(__dirname,'public')));

const PORT=process.env.PORT||10000;
const ADMIN_PASSWORD=process.env.ADMIN_PASSWORD;
if(!ADMIN_PASSWORD) console.warn('WARNING: ADMIN_PASSWORD is not set. Admin login will be disabled.');

const users=new Map(), requests=new Map(), sessions=new Map(), adminSessions=new Set();
const messages=[], sockets=new Map();
const group={name:'Midnight Group Chat',locked:false,picture:null};
let seq=1;
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
function token(){return crypto.randomBytes(32).toString('hex')}
function cleanUser(u){return {id:u.id,username:u.username,role:u.role,online:u.online}}
function members(){return [...users.values()].filter(u=>u.approved).map(cleanUser)}
function broadcast(){io.emit('group-state',{members:members(),groupPic:group.picture})}
function auth(req){const t=(req.headers.authorization||'').replace('Bearer ','');return sessions.get(t)}
function admin(req){const t=(req.headers.authorization||'').replace('Bearer ','');return adminSessions.has(t)}
function allowedName(n){return /^[\p{L}\p{N}_ .-]{2,24}$/u.test(n)}
function safeMessage(m){return {...m,media:m.media||null}}

app.get('/api/me',(req,res)=>{const u=auth(req);if(!u||!u.approved)return res.status(401).json({error:'Not approved'});res.json({user:cleanUser(u)})});
app.get('/api/group',(req,res)=>res.json({name:group.name,members:members().length,online:members().filter(x=>x.online).length,locked:group.locked,picture:group.picture}));
app.post('/api/join',(req,res)=>{
 const username=String(req.body.username||'').trim();
 if(!allowedName(username))return res.status(400).json({error:'Use 2–24 letters, numbers, spaces, dots, hyphens or underscores.'});
 if([...users.values()].some(u=>u.username.toLowerCase()===username.toLowerCase()&&u.approved))return res.status(409).json({error:'Username already exists.'});
 const existing=[...requests.values()].find(x=>x.username.toLowerCase()===username.toLowerCase()&&x.status==='pending');
 if(existing)return res.json({status:'pending',requestId:existing.id});
 const id=crypto.randomUUID(),r={id,username,createdAt:Date.now(),status:'pending'};requests.set(id,r);res.json({status:'pending',requestId:id});
 io.emit('system',{text:`New join request: @${username}`});
});
app.post('/api/admin/login',(req,res)=>{if(!ADMIN_PASSWORD)return res.status(503).json({error:'ADMIN_PASSWORD is not configured on the server.'});if(String(req.body.password||'')!==ADMIN_PASSWORD)return res.status(401).json({error:'Invalid password'});const t=token();adminSessions.add(t);res.json({token:t})});
app.get('/api/admin/me',(req,res)=>res.json({ok:admin(req)}));
app.get('/api/admin/requests',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});res.json({requests:[...requests.values()].filter(x=>x.status==='pending')})});
app.get('/api/admin/members',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});res.json({members:[...users.values()].filter(u=>u.approved).map(cleanUser)})});
app.post('/api/admin/requests/:id',(req,res)=>{
 if(!admin(req))return res.status(401).json({error:'Unauthorized'});const r=requests.get(req.params.id);if(!r)return res.status(404).json({error:'Request not found'});
 if(req.body.action==='approve'){r.status='approved';const u={id:crypto.randomUUID(),username:r.username,role:'member',approved:true,online:false};users.set(u.id,u);const t=token();sessions.set(t,u);r.userId=u.id;r.token=t;io.emit('pending-approved',{user:cleanUser(u),token:t});}
 else r.status='rejected';
 res.json({ok:true});
});
app.patch('/api/admin/members/:id',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});const u=users.get(req.params.id);if(!u||!u.approved)return res.status(404).json({error:'Member not found'});if(req.body.role&&!['member','admin'].includes(req.body.role))return res.status(400).json({error:'Invalid role'});u.role=req.body.role;broadcast();res.json({ok:true})});
app.delete('/api/admin/members/:id',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});const u=users.get(req.params.id);if(!u)return res.status(404).json({error:'Member not found'});if(u.id===ownerId)return res.status(400).json({error:'Cannot remove owner'});users.delete(u.id);for(const [t,x] of sessions)if(x.id===u.id)sessions.delete(t);for(const [sid,x] of sockets)if(x.userId===u.id){io.sockets.sockets.get(sid)?.disconnect(true)}broadcast();res.json({ok:true})});
app.patch('/api/admin/group',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});if(typeof req.body.locked==='boolean')group.locked=req.body.locked;if(typeof req.body.picture==='string'&&req.body.picture.length<7e6)group.picture=req.body.picture;broadcast();res.json({ok:true})});
app.delete('/api/admin/messages',(req,res)=>{if(!admin(req))return res.status(401).json({error:'Unauthorized'});messages.length=0;io.emit('system',{text:'All messages were cleared by an administrator.'});res.json({ok:true})});

io.on('connection',socket=>{
 socket.on('auth',({token:t})=>{
   const u=sessions.get(t);if(!u||!u.approved)return;
   u.online=true;sockets.set(socket.id,{userId:u.id});socket.userId=u.id;
   socket.emit('auth-ok',{user:cleanUser(u),members:members(),messages:messages.map(safeMessage)});
   broadcast();socket.broadcast.emit('system',{text:`@${u.username} is online`});
 });
 socket.on('admin-auth',({token:t})=>{if(adminSessions.has(t))socket.join('admins')});
 socket.on('send-message',data=>{
   const u=users.get(socket.userId);if(!u||!u.approved)return;
   if(group.locked&&u.role==='member')return socket.emit('system',{text:'Chat is currently locked by an admin.'});
   const m={id:String(seq++),userId:u.id,username:u.username,text:String(data.text||'').slice(0,5000),type:['image','sticker','audio'].includes(data.type)?data.type:'text',media:typeof data.media==='string'&&data.media.length<7e6?data.media:null,viewOnce:!!data.viewOnce,replyTo:data.replyTo||null,createdAt:Date.now(),reactions:{}};
   messages.push(m);if(messages.length>500)messages.shift();io.emit('message',safeMessage(m));
 });
 socket.on('react',({messageId,emoji})=>{const m=messages.find(x=>x.id===String(messageId));if(!m)return;m.reactions=m.reactions||{};m.reactions[emoji]=(m.reactions[emoji]||0)+1;io.emit('message-reaction',{messageId:m.id,reactions:m.reactions})});
 socket.on('view-once',({messageId})=>{const m=messages.find(x=>x.id===String(messageId));if(!m||!m.viewOnce)return;if(m.userId!==socket.userId)m.media=null;io.emit('view-once-opened',{messageId:m.id,userId:socket.userId})});

 socket.on('typing',typing=>{
   const u=users.get(socket.userId); if(!u)return;
   socket.broadcast.emit('typing',{userId:u.id,username:u.username,typing:!!typing,online:members().filter(x=>x.online).length});
 });
 socket.on('pin-message',({messageId})=>{
   const u=users.get(socket.userId),m=messages.find(x=>x.id===String(messageId));
   if(!u||!m||!['admin','owner'].includes(u.role))return;
   m.pinned=true;io.emit('message-pinned',safeMessage(m));
 });
 socket.on('delete-message',({messageId})=>{
   const u=users.get(socket.userId); if(!u||!['admin','owner'].includes(u.role))return;
   const i=messages.findIndex(x=>x.id===String(messageId)); if(i<0)return;
   messages.splice(i,1);io.emit('message-deleted',String(messageId));
 });

 socket.on('disconnect',()=>{const s=sockets.get(socket.id);if(s){const u=users.get(s.userId);if(u){u.online=false;broadcast();io.emit('system',{text:`@${u.username} went offline`})}sockets.delete(socket.id)}});
});

app.get('/admin',(req,res)=>res.sendFile(path.join(__dirname,'public','admin.html')));
app.get('/health',(req,res)=>res.json({status:'ok',members:members().length,online:members().filter(x=>x.online).length}));
server.listen(PORT,()=>console.log(`Midnight Group Chat listening on ${PORT}`));