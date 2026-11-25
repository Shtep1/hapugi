// Хапуги: сервер для Render. Отдаёт игру и держит комнаты для сетевой игры.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };

const server = http.createServer((req, res) => {
  let name = decodeURIComponent((req.url || '/').split('?')[0]);
  if (name === '/' || name === '') name = '/index.html';
  if (name === '/healthz') { res.writeHead(200); return res.end('ok'); }
  let file = path.join(PUB, path.basename(name));
  if (!fs.existsSync(file)) file = path.join(__dirname, path.basename(name));
  if (path.basename(file) === 'server.js' || path.basename(file) === 'package.json') file = path.join(PUB, 'nope');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Не найдено'); }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
const rooms = new Map(); // имя комнаты -> Map(id -> {ws, p})
const pending = new Set();

function broadcast(room) {
  if (pending.has(room)) return;
  pending.add(room);
  setTimeout(() => {
    pending.delete(room);
    const members = rooms.get(room);
    if (!members) return;
    const msg = JSON.stringify({ t: 'all', list: [...members].map(([peer, m]) => ({ peer, p: m.p })) });
    for (const m of members.values()) if (m.ws.readyState === 1) m.ws.send(msg);
  }, 15);
}

wss.on('connection', ws => {
  const id = crypto.randomBytes(6).toString('hex');
  let room = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let d; try { d = JSON.parse(raw); } catch { return; }
    if (!d || typeof d !== 'object') return;
    if (d.t === 'join' && !room && typeof d.room === 'string' && /^[a-z0-9][a-z0-9_.-]{0,59}$/.test(d.room)) {
      room = d.room;
      if (!rooms.has(room)) rooms.set(room, new Map());
      const members = rooms.get(room);
      if (members.size >= 12) { ws.send(JSON.stringify({ t: 'full' })); ws.close(); room = null; return; }
      members.set(id, { ws, p: {} });
      ws.send(JSON.stringify({ t: 'hi', id }));
      broadcast(room);
    } else if (d.t === 'p' && room && d.p && typeof d.p === 'object' && !Array.isArray(d.p)) {
      const members = rooms.get(room);
      if (members && members.has(id)) { members.get(id).p = d.p; broadcast(room); }
    }
  });
  ws.on('close', () => {
    if (!room) return;
    const members = rooms.get(room);
    if (!members) return;
    members.delete(id);
    if (members.size === 0) rooms.delete(room); else broadcast(room);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Хапуги работают на порту ' + PORT));
