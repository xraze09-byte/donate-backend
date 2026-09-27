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
// room -> array of recent 'alert' messages (short-lived replay so an overlay that
// drops/reconnects right when a donation gets approved doesn't permanently miss
// that alert — unlike `donation` records, alerts were never backlogged before,
// so a reconnect at the wrong instant silently dropped the voice/card announcement).
const alertBacklog = new Map();
const ALERT_TTL_MS = 2 * 60 * 1000;   // 2 min — long enough to survive a reconnect blip
const ALERT_MAX = 8;                  // cap per room so it can't grow unbounded
// room -> last activity timestamp, used to garbage-collect abandoned channels
// (cfgStore/backlog/alertBacklog otherwise live forever in memory, even for
// channels nobody has used in months — a slow leak on a long-running process).
const lastSeen = new Map();
const BACKLOG_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const ROOM_IDLE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days with zero traffic = abandoned

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
function pruneAlerts(ch) {
  const arr = alertBacklog.get(ch);
  if (!arr) return;
  const now = Date.now();
  while (arr.length && now - (arr[0]._at || 0) > ALERT_TTL_MS) arr.shift();
  if (!arr.length) alertBacklog.delete(ch);
}
// Periodic sweep for channels with no connected clients and no traffic in a long
// time — frees cfgStore/backlog/alertBacklog memory for streamers who stopped
// using the app instead of holding it forever for the life of the process.
setInterval(() => {
  const now = Date.now();
  for (const [ch, ts] of lastSeen) {
    const hasPeers = rooms.has(ch) && rooms.get(ch).size > 0;
    if (!hasPeers && now - ts > ROOM_IDLE_MS) {
      backlog.delete(ch); cfgStore.delete(ch); alertBacklog.delete(ch); lastSeen.delete(ch);
    }
  }
}, 60 * 60 * 1000);

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  res.writeHead(404);
  res.end('not found');
});

// จำกัดขนาดข้อความ — กันข้อความใหญ่ผิดปกติ (เช่น ส่ง base64 รูปเข้ามาผิด) ทำให้หน่วยความจำบวม
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

// บัคร้ายแรง: เดิมไม่มี wss.on('error', ...) — ถ้า underlying server มีปัญหา (เช่น พอร์ตชน)
// จะโยน exception ที่ไม่มีใครจับ แล้ว "ws" ไลบรารีก็ทำแบบเดียวกันกับทุก connection ที่ error
// (ดูด้านล่างใน connection handler) ซึ่งจะทำให้ Node process ทั้งตัวล้ม กระทบทุกห้อง/ทุกสายที่แอดมิน
// อยู่พร้อมกัน ไม่ใช่แค่ connection ที่เจอปัญหา — แก้โดย log แทนการปล่อยให้ throw
wss.on('error', (err) => { console.error('wss error:', err && err.message); });
server.on('error', (err) => { console.error('http server error:', err && err.message); });

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
  lastSeen.set(ch, Date.now());

  // บัคร้ายแรง (แก้แล้ว): 'ws' (จาก lib "ws") ยิง 'error' event ทุกครั้งที่ socket มีปัญหา
  // ระดับ TCP/โปรโตคอล (เน็ตมือถือหลุด, เฟรมเสีย ฯลฯ) — ถ้าไม่มี listener จับไว้ Node.js จะ throw
  // exception ที่ไม่มีใครจับ = process ทั้งตัวล่ม พา "ทุกห้อง ทุกสาย" ที่ต่ออยู่ตอนนั้นหลุดหมด
  // ไม่ใช่แค่ client ที่มีปัญหา — เป็นสาเหตุที่เป็นไปได้สูงของอาการ "เซิร์ฟเวอร์ล่มเอง เงียบ ๆ
  // โดเนทหยุดเข้าดื้อ ๆ" ตอนนี้แค่ log แล้วปิด socket นั้นทิ้งแทนที่จะปล่อยให้ล้มทั้ง process
  ws.on('error', (err) => {
    console.error('ws error on ch=' + ch + ':', err && err.message);
    try { ws.terminate(); } catch (e) {}
  });

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

  // บัค (แก้แล้ว): เดิม 'alert' (การ์ด/เสียงอ่านโดเนท) ไม่ถูกเก็บ backlog เลย ต่างจาก
  // 'donation' — ถ้า overlay (OBS) หลุดต่อกลับพอดีตอนแอดมินกดอนุมัติ จะพลาดอ่านยอดนั้นไปเลย
  // ตลอดกาล ไม่มีทางกู้คืน ตอนนี้ replay alert ล่าสุดไม่กี่รายการ (ภายใน 2 นาที) ให้ด้วย
  pruneAlerts(ch);
  const alerts = alertBacklog.get(ch);
  if (alerts) for (const msg of alerts) { try { ws.send(JSON.stringify(msg)); } catch (e) {} }

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!msg || !msg.t) return;
    msg._at = Date.now();
    lastSeen.set(ch, msg._at);

    // Track pending donations in the backlog; drop them once approved/rejected/deleted.
    if (msg.t === 'donation' && msg.d && msg.d.rec && msg.d.rec.id) {
      getBacklog(ch).set(msg.d.rec.id, msg);
    } else if (msg.t === 'resolved' && msg.d && msg.d.id) {
      getBacklog(ch).delete(msg.d.id);
    } else if (msg.t === 'alert' && msg.d) {
      // เก็บ alert ล่าสุดไว้สั้น ๆ เผื่อ overlay หลุดต่อกลับพอดีจังหวะที่ยิง alert นี้
      pruneAlerts(ch);
      if (!alertBacklog.has(ch)) alertBacklog.set(ch, []);
      const arr = alertBacklog.get(ch);
      arr.push(msg);
      if (arr.length > ALERT_MAX) arr.shift();
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
