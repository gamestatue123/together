const express = require('express'), http = require('http'), { Server } = require('socket.io');
const app = express(), srv = http.createServer(app), io = new Server(srv);
app.use(express.static('public'));
const KEY = process.env.YOUTUBE_API_KEY;
const idOf = s => { const m = s.match(/(?:youtu\.be\/|v=|embed\/|shorts\/)([\w-]{11})/); return m ? m[1] : /^[\w-]{11}$/.test(s) ? s : null; };
const dec = s => s.replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');

// Search (needs API key) or resolve a pasted link (no key needed)
app.get('/api/search', async (req, res) => {
  const q = String(req.query.q || '').trim().slice(0, 200);
  if (!q) return res.json([]);
  try {
    const id = idOf(q);
    if (id) {
      const r = await fetch('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent('https://youtu.be/' + id));
      if (!r.ok) return res.json({ error: 'Video not found or not embeddable.' });
      const j = await r.json();
      return res.json([{ vid: id, title: j.title, channel: j.author_name, link: true }]);
    }
    if (!KEY) return res.json({ error: 'Search is off. Paste a YouTube link instead.' });
    const u = 'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&maxResults=8&q=' + encodeURIComponent(q) + '&key=' + KEY;
    const j = await (await fetch(u)).json();
    if (j.error) return res.json({ error: 'Daily search limit reached. Paste a link instead.' });
    res.json(j.items.map(i => ({ vid: i.id.videoId, title: dec(i.snippet.title), channel: dec(i.snippet.channelTitle) })));
  } catch (e) { res.json({ error: 'Search failed. Try again.' }); }
});

const rooms = new Map();
const rid = () => Math.random().toString(36).slice(2, 9);
const newCode = () => { let c; do c = Array.from({ length: 5 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.random() * 24 | 0]).join(''); while (rooms.has(c)); return c; };
const posNow = r => r.playing ? r.pos + (Date.now() - r.at) / 1000 : r.pos;
const pub = r => ({
  code: r.code, host: r.users.get(r.host)?.uid, queue: r.queue, cur: r.cur, playing: r.playing,
  pos: r.pos, at: r.at, gc: r.gc, ga: r.ga,
  users: [...r.users.values()].map(u => ({ uid: u.uid, name: u.name }))
});
const push = r => io.to(r.code).emit('state', pub(r));

io.on('connection', s => {
  let r, u;
  const isHost = () => r && u && r.host === u.token;
  const canCtl = () => isHost() || (r && r.gc);
  const start = it => { r.cur = it; r.pos = 0; r.at = Date.now(); r.playing = !!it; };
  const next = () => start(r.queue.shift() || null);

  s.on('time', cb => typeof cb === 'function' && cb(Date.now()));

  s.on('join', ({ code, name, token } = {}, cb) => {
    if (typeof token !== 'string' || token.length < 8) return cb({ err: 'Bad request' });
    name = String(name || 'Guest').trim().slice(0, 20) || 'Guest';
    code = String(code || '').toUpperCase().trim();
    if (code) { r = rooms.get(code); if (!r) return cb({ err: 'Room not found' }); }
    else { r = { code: newCode(), host: token, users: new Map(), queue: [], cur: null, playing: false, pos: 0, at: Date.now(), gc: false, ga: true }; rooms.set(r.code, r); }
    u = r.users.get(token);
    if (u) { clearTimeout(u.t); u.name = name; } else { u = { token, uid: rid(), name }; r.users.set(token, u); }
    u.sid = s.id; s.join(r.code);
    cb({ uid: u.uid, code: r.code }); push(r);
  });

  s.on('add', v => {
    if (!r || !(isHost() || r.ga) || !v || !/^[\w-]{11}$/.test(v.vid) || r.queue.length >= 100) return;
    const it = { id: rid(), vid: v.vid, title: String(v.title || 'Untitled').slice(0, 120), by: u.name, byUid: u.uid };
    if (!r.cur) start(it); else r.queue.push(it);
    push(r);
  });

  s.on('ctl', a => {
    if (!r || !canCtl() || !a) return;
    if (a.type === 'play' && r.cur && !r.playing) { r.at = Date.now(); r.playing = true; }
    else if (a.type === 'pause' && r.playing) { r.pos = posNow(r); r.playing = false; }
    else if (a.type === 'seek' && r.cur) { r.pos = Math.max(0, +a.t || 0); r.at = Date.now(); }
    else if (a.type === 'next') next();
    else if (a.type === 'jump') { const i = r.queue.findIndex(x => x.id === a.id); if (i < 0) return; start(r.queue.splice(i, 1)[0]); }
    else return;
    push(r);
  });

  s.on('ended', id => { if (r && r.cur && r.cur.id === id) { next(); push(r); } });

  s.on('remove', id => {
    if (!r) return;
    const i = r.queue.findIndex(x => x.id === id);
    if (i < 0 || !(isHost() || r.queue[i].byUid === u.uid)) return;
    r.queue.splice(i, 1); push(r);
  });

  s.on('settings', v => { if (!isHost() || !v) return; r.gc = !!v.gc; r.ga = !!v.ga; push(r); });

  s.on('kick', uid => {
    if (!isHost()) return;
    const k = [...r.users.values()].find(x => x.uid === uid);
    if (!k || k === u) return;
    io.to(k.sid).emit('kicked'); io.sockets.sockets.get(k.sid)?.leave(r.code);
    clearTimeout(k.t); r.users.delete(k.token); push(r);
  });

  s.on('disconnect', () => {
    if (!r || !u) return;
    const room = r, me = u;
    me.t = setTimeout(() => {              // 45s grace so a phone lock doesn't kick you
      room.users.delete(me.token);
      if (!room.users.size) return rooms.delete(room.code);
      if (room.host === me.token) room.host = room.users.keys().next().value;  // host passes to oldest member
      push(room);
    }, 45000);
  });
});

srv.listen(process.env.PORT || 3000, () => console.log('Running on http://localhost:' + (process.env.PORT || 3000)));
