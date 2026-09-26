/**
 * RZ CLAN — Realtime relay server
 * Plain WebSocket relay + tiny backlog buffer so the admin queue catches up
 * on donations that arrived while the admin tab was offline/reconnecting.
 *
 * This server holds NO business logic on purpose: the HTML app already
 * knows how to build/approve/reject donations. This process just relays
 * `emit()` messages between browsers that share the same channel key (`ch`),
 * and replays a short backlog of pending donations to anyone who (re)connects.
 */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
// Optional shared secret. If set, clients must connect with ?tok=THIS_VALUE.
// The channel key (`ch`) in the URL already acts as a capability secret
// (it's the same "Overlay Secret Key" the HTML app generates), but RELAY_TOKEN
// adds a second layer if you want it.
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';

// room (channel key) -> Set<ws>
const rooms = new Map();
// room -> Map<donationId, message>  (backlog of NOT-yet-approved donations)
const backlog = new Map();
// room -> last payment config broadcast by the admin ({pp, payee, goal, min, reqSlip})
// Sent to every NEW connection so donors who open the bare URL still get the
// PromptPay number without needing the donate link or the admin being online.
const cfgStore = new Map();
const BACKLOG_TTL_MS = 24 * 60 * 60 * 1000; // 24h

function getRoom(ch) {
  if (!rooms.has(ch)) rooms.set(ch, new Set());
  return rooms.get(ch);
}
function getBacklog(ch) {
  if (!backlog.has(ch)) backlog.set(ch, new Map());
  return backlog.get(ch);
}

function pruneBacklog(ch) {
  const b = getBacklog(ch);
  const now = Date.now();
  for (const [id, msg] of b) {
    if (now - (msg._at || 0) > BACKLOG_TTL_MS) b.delete(id);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  const ch = url.searchParams.get('ch') || 'rzc_x';
  const tok = url.searchParams.get('tok') || '';

  if (RELAY_TOKEN && tok !== RELAY_TOKEN) {
    ws.close(4001, 'bad token');
    return;
  }

  ws.ch = ch;
  ws.isAlive = true;
  getRoom(ch).add(ws);

  // Replay backlog (pending donations) so a reconnecting admin catches up.
  pruneBacklog(ch);
  for (const msg of getBacklog(ch).values()) {
    try { ws.send(JSON.stringify(msg)); } catch (e) {}
  }

  // Replay stored payment config so fresh donors get the PromptPay number.
  const cfgMsg = cfgStore.get(ch);
  if (cfgMsg) {
    try { ws.send(JSON.stringify(cfgMsg)); } catch (e) {}
  }

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || !msg.t) return;
    msg._at = Date.now();

    // Track pending donations in the backlog; drop them once approved/rejected/deleted.
    if (msg.t === 'donation' && msg.d && msg.d.rec && msg.d.rec.id) {
      getBacklog(ch).set(msg.d.rec.id, msg);
    } else if (msg.t === 'resolved' && msg.d && msg.d.id) {
      getBacklog(ch).delete(msg.d.id);
    } else if (msg.t === 'cfg' && msg.d && typeof msg.d === 'object') {
      // keep only donor-facing fields, never touch the channel key
      const d = msg.d;
      cfgStore.set(ch, { t: 'cfg', d: {
        pp: String(d.pp || ''), payee: String(d.payee || ''),
        goal: Number(d.goal) || 0, min: Number(d.min) || 0,
        reqSlip: !!d.reqSlip
      }});
    }

    // Relay to every OTHER client in the same room.
    const peers = getRoom(ch);
    for (const client of peers) {
      if (client !== ws && client.readyState === client.OPEN) {
        try { client.send(JSON.stringify(msg)); } catch (e) {}
      }
    }
  });

  ws.on('close', () => {
    const peers = rooms.get(ch);
    if (peers) {
      peers.delete(ws);
      if (peers.size === 0) rooms.delete(ch);
    }
  });
});

// Heartbeat: ping every 30s, drop dead sockets. Keeps free-tier hosts from
// idling the connection out and detects phones that dropped without a close frame.
const HEARTBEAT_MS = 30000;
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, HEARTBEAT_MS);

wss.on('close', () => clearInterval(interval));

server.listen(PORT, () => {
  console.log(`RZ relay listening on :${PORT} (ws path /ws)`);
});
