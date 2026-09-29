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
const { handleTTS, handleTTSHealth } = require('./tts');

const PORT = process.env.PORT || 8080;
// Optional shared secret. If set, clients must connect with ?tok=THIS_VALUE.
// The channel key (`ch`) in the URL already acts as a capability secret
// (it's the same "Overlay Secret Key" the HTML app generates), but RELAY_TOKEN
// adds a second layer if you want it. Stays OPT-IN (unset by default) so this
// deploy doesn't break unless you deliberately turn it on.
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';
// Read-only token for the separate queue service (bridge.js). A connection with this token
// gets scope 'listener': it RECEIVES everything the room broadcasts (minus slip images) but
// every message it sends is dropped. Leaking it lets an attacker read pending donations, never
// forge alerts/approvals. MUST differ from RELAY_TOKEN. Unset = feature off (zero behaviour change).
const LISTEN_TOKEN = process.env.LISTEN_TOKEN || '';
// Optional Origin allowlist, comma-separated (e.g. "https://your-app.vercel.app").
// Also OPT-IN — unset means no restriction, same posture as RELAY_TOKEN.
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',').map((s) => s.trim()).filter(Boolean);
// เปิด ALLOWED_ORIGINS ไว้ก่อนแบบ "แค่ log ไม่บล็อกจริง" (ENFORCE_ORIGIN ยังไม่ตั้ง = 0) เพื่อดู
// ของจริงว่ามี Origin อะไรมาเชื่อมต่อบ้างก่อนบังคับ — กันเคสเดามั่ว/ลืมโดเมนที่ใช้จริงบางตัว
// (เช่น OBS ที่โหลดไฟล์ .html ตรง ๆ จะส่ง Origin เป็น null/file:// ไม่ใช่โดเมนเว็บ)
const ENFORCE_ORIGIN = process.env.ENFORCE_ORIGIN === '1';

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

// บัค (แก้แล้ว): เดิม `ch` ไม่ตรวจรูปแบบเลย และห้องไม่มีเพดานจำนวน — สคริปต์ที่เปิด
// ?ch=<uuid สุ่ม> วนซ้ำ ๆ จะสร้าง Map entry ใหม่ทุกครั้ง (5 Map ต่อห้อง) จนหน่วยความจำเต็มได้
// ตอนนี้ตรวจรูปแบบ + จำกัดจำนวนห้องทั้งหมด + จำนวนคนต่อห้อง (คีย์เดิมที่ใช้อยู่ยังผ่านได้ปกติ)
const CH_RE = /^[A-Za-z0-9_-]{3,80}$/;
const MAX_ROOMS = 500;
const MAX_PEERS_PER_ROOM = 30;

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
const gcTimer = setInterval(() => {
  const now = Date.now();
  for (const [ch, ts] of lastSeen) {
    const hasPeers = rooms.has(ch) && rooms.get(ch).size > 0;
    if (!hasPeers && now - ts > ROOM_IDLE_MS) {
      backlog.delete(ch); cfgStore.delete(ch); alertBacklog.delete(ch); lastSeen.delete(ch);
    }
  }
}, 60 * 60 * 1000);

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }
  // TTS proxy (F-TTS-01): overlay ใน OBS ไม่มี speech engine เลย ต้องสังเคราะห์เสียงที่นี่
  // แล้วส่ง MP3 กลับไปให้ overlay เล่นผ่าน <audio> — ดู tts.js สำหรับรายละเอียด
  if (req.url.startsWith('/tts/health')) return handleTTSHealth(req, res);
  if (req.url.startsWith('/tts')) return handleTTS(req, res);
  res.writeHead(404);
  res.end('not found');
});

// จำกัดขนาดข้อความ — 512KB พอให้สลิปโอนเงิน (base64 จากรูปภาพมือถือ ~200-800KB ก่อนบีบ) ผ่านได้
// เดิม 64KB เตี้ยเกินไป ทุกสลิปที่แนบมาจะโดนตัดการเชื่อมต่อ (code 1009) แทนที่จะส่งถึงแอดมิน
// ซึ่งอาการที่เห็นคือ "หลุดสุ่ม ๆ" ไม่ใช่ error ที่บอกสาเหตุตรง ๆ — เพิ่ม perMessageDeflate ช่วยลด
// แบนด์วิดท์ของสลิป base64 ที่ตัวมันบีบได้ดี (ข้อความ JSON ทั่วไปก็ได้ประโยชน์ด้วย)
const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 512 * 1024,
  perMessageDeflate: { threshold: 1024 },
  verifyClient(info, cb) {
    // บัค (แก้แล้ว): เดิมไม่เช็ค Origin เลย — ถ้าตั้ง ALLOWED_ORIGINS ไว้ (ยังเป็น opt-in
    // เหมือน RELAY_TOKEN เพื่อไม่ให้ deploy นี้พังของเดิมทันทีถ้ายังไม่ได้ตั้งค่า) จะบล็อกได้
    if (ALLOWED_ORIGINS.length) {
      const origin = info.origin || 'null';
      const originOk = ALLOWED_ORIGINS.includes(origin);
      if (!originOk) {
        console.warn(`[origin] ${ENFORCE_ORIGIN ? 'BLOCKED' : 'WOULD-BLOCK (dry-run, set ENFORCE_ORIGIN=1 to actually block)'}: ${origin}`);
        if (ENFORCE_ORIGIN) return cb(false, 403, 'bad origin');
      }
    }
    let u;
    try { u = new URL(info.req.url, 'http://x'); } catch (e) { return cb(false, 400, 'bad url'); }
    const ch = u.searchParams.get('ch') || 'rzc_x';
    if (!CH_RE.test(ch)) return cb(false, 400, 'bad channel');
    if (!rooms.has(ch) && rooms.size >= MAX_ROOMS) return cb(false, 503, 'too many rooms');
    if ((rooms.get(ch)?.size || 0) >= MAX_PEERS_PER_ROOM) return cb(false, 503, 'room full');
    cb(true);
  },
});

// บัคร้ายแรง: เดิมไม่มี wss.on('error', ...) — ถ้า underlying server มีปัญหา (เช่น พอร์ตชน)
// จะโยน exception ที่ไม่มีใครจับ แล้ว "ws" ไลบรารีก็ทำแบบเดียวกันกับทุก connection ที่ error
// (ดูด้านล่างใน connection handler) ซึ่งจะทำให้ Node process ทั้งตัวล้ม กระทบทุกห้อง/ทุกสายที่แอดมิน
// อยู่พร้อมกัน ไม่ใช่แค่ connection ที่เจอปัญหา — แก้โดย log แทนการปล่อยให้ throw
wss.on('error', (err) => { console.error('wss error:', err && err.message); });
server.on('error', (err) => { console.error('http server error:', err && err.message); });
// บัค (แก้แล้ว): exception ที่หลุดจาก event handler อื่น ๆ (เช่น bug เล็ก ๆ ใน message handler)
// ก็ทำให้ process ล่มได้เหมือนกันถ้าไม่มี guard เหล่านี้ — log แทนปล่อยให้ process ตาย
process.on('unhandledRejection', (err) => { console.error('unhandledRejection:', err); });
process.on('uncaughtException', (err) => { console.error('uncaughtException:', err); });

// ── Rate limit: token bucket ต่อ connection (10 msg/s ต่อเนื่อง, burst 25) ──────────
// เดิมไม่มีการจำกัดเลย — client เดียวส่งถี่แค่ไหนก็ได้ ทุกข้อความ JSON.stringify ซ้ำต่อ
// ผู้ฟังทุกคนในห้อง (O(peers) ต่อข้อความ) ถ้าถูกยิงถี่มากพอ CPU จะอิ่มจน heartbeat พลาด
// แล้วเซิร์ฟเวอร์เริ่มตัด connection ปกติทิ้งไปเอง
const RATE_CAP = 25, RATE_REFILL = 10;
function allow(ws) {
  const now = Date.now();
  ws._tok = Math.min(RATE_CAP, (ws._tok ?? RATE_CAP) + ((now - (ws._last ?? now)) / 1000) * RATE_REFILL);
  ws._last = now;
  if (ws._tok < 1) return false;
  ws._tok -= 1;
  return true;
}

// ── S-04: whitelist sanitizer — เดิม donation/alert/resolved ถูก relay ตรง ๆ ไม่ตรวจ
// เลยแม้แต่ field เดียว (มีแค่ cfg ที่ sanitize) ทำให้ peer ที่รู้ ch ส่งอะไรก็ได้เข้าไปที่หน้า
// จอแอดมิน/โอเวอร์เลย์ ซึ่ง render ด้วย innerHTML — เพิ่ม whitelist ที่ "รักษาทุก field ที่
// หน้าเว็บใช้จริง" (ts/ref/status ที่ renderQueue ใช้เรียงคิว/แสดงวันที่ ฯลฯ) แค่บังคับ type/
// ความยาว ไม่ใช่ตัดทิ้งแบบ snippet ตัวอย่างที่มาด้วย ซึ่งจะทำให้คิวแอดมินพังเพราะ ts/ref หายไป
const clampStr = (v, n) => String(v == null ? '' : v).slice(0, n);
// จำกัดความยาว base64 ของสลิปให้พอดีกับ maxPayload 512KB (เผื่อ overhead ของ JSON โครงสร้าง)
const SLIP_RE = /^data:image\/(png|jpe?g|webp);base64,[A-Za-z0-9+/=]{1,600000}$/;
const STATUSES = ['pending', 'approved', 'rejected'];
// alert/cfg/resolved ควรมาจาก "แอดมิน" เท่านั้น — donation/sync มาจากใครก็ได้ (โดนอร์)
const ADMIN_TYPES = new Set(['alert', 'cfg', 'resolved']);
const PUBLIC_TYPES = new Set(['donation', 'sync']);

// q = optional queue request attached by the donate form: { name, uid }. uid must be a Free Fire
// UID (6-14 digits, same rule as Queue-system/queue.js). Anything malformed is DROPPED (the
// donation itself still goes through untouched) - a bad q must never block a payment.
function sanitizeQ(q) {
  if (!q || typeof q !== 'object') return undefined;
  const uid = String(q.uid == null ? '' : q.uid).replace(/\D/g, '');
  if (!/^\d{6,14}$/.test(uid)) return undefined;
  return { name: clampStr(q.name, 24), uid };
}

function sanitizeRec(src) {
  if (!src || typeof src !== 'object') return null;
  const id = clampStr(src.id, 64);
  if (!id) return null;
  return {
    id,
    name: clampStr(src.name, 40) || 'ANONYMOUS',
    message: clampStr(src.message, 200),
    amount: Math.min(Math.max(Number(src.amount) || 0, 0), 1000000),
    ts: Number(src.ts) || Date.now(),
    ref: clampStr(src.ref, 32),
    // บัคเพิ่ม (พบระหว่างแก้ S-04): เดิม sanitizer ยอม client อ้าง status:'approved' มาตรงๆ
    // ใน 'donation' message ได้เลย — ทั้งที่ทุก path ที่ถูกต้องของแอปสร้าง donation ใหม่เป็น
    // 'pending' เสมอ (การอนุมัติจริงมาจาก 'resolved' type เท่านั้น) เท่ากับใครก็ประกาศยอด
    // ตัวเองว่า "อนุมัติแล้ว" ได้เลยโดยไม่ผ่านแอดมิน — บังคับเป็น pending เสมอตรงนี้แทน
    status: 'pending',
    slip: !!src.slip,
    sha: clampStr(src.sha, 64), // F-06: แฮชสลิปไว้เทียบซ้ำ
    ...(sanitizeQ(src.q) ? { q: sanitizeQ(src.q) } : {}),
  };
}
function sanitize(msg) {
  const t = msg.t;
  if (t === 'donation') {
    const rec = sanitizeRec(msg.d && msg.d.rec);
    if (!rec) return null;
    const rawSlip = msg.d && msg.d.slip;
    const slip = typeof rawSlip === 'string' && SLIP_RE.test(rawSlip) ? rawSlip : null;
    return { t, _at: Date.now(), d: { rec, slip } };
  }
  if (t === 'alert') {
    const src = msg.d;
    if (!src || typeof src !== 'object') return null;
    return {
      t, _at: Date.now(),
      d: {
        name: clampStr(src.name, 40) || 'ANONYMOUS',
        amount: Math.min(Math.max(Number(src.amount) || 0, 0), 1000000),
        message: clampStr(src.message, 200),
      },
    };
  }
  if (t === 'resolved') {
    const src = msg.d;
    const id = clampStr(src && src.id, 64);
    if (!id) return null;
    return {
      t, _at: Date.now(),
      d: { id, status: ['approved', 'rejected'].includes(src.status) ? src.status : 'approved' },
    };
  }
  if (t === 'cfg') {
    const d = (msg.d && typeof msg.d === 'object') ? msg.d : {};
    return {
      t, _at: Date.now(),
      d: {
        pp: clampStr(d.pp, 20), payee: clampStr(d.payee, 25),
        goal: Math.max(0, Number(d.goal) || 0), min: Math.max(0, Number(d.min) || 0),
        reqSlip: !!d.reqSlip,
      },
    };
  }
  if (t === 'sync') return { t, _at: Date.now(), d: {} };
  return null; // ไม่รู้จัก type นี้ — ไม่ relay
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  const ch = url.searchParams.get('ch') || 'rzc_x';
  const tok = url.searchParams.get('tok') || '';

  // ── Scope assignment (ห้ามเตะ donor ทิ้ง: donor ไม่มี token เป็นเรื่องปกติ) ──
  //   LISTEN_TOKEN ตรง                 -> 'listener' (อ่านอย่างเดียว, ส่งอะไรไม่ได้)
  //   ไม่ตั้ง RELAY_TOKEN              -> 'admin'    (พฤติกรรมเดิม 100%)
  //   RELAY_TOKEN ตรง                  -> 'admin'
  //   ไม่มี token / token อื่น         -> 'public'   (ส่งได้แค่ donation/sync)
  // บัคเดิม: ตั้ง RELAY_TOKEN แล้ว connection ที่ไม่มี token (= หน้าโดเนทของผู้บริจาคทุกคน)
  // ถูก close(4001) ทิ้ง -> โดเนทหยุดเข้าทั้งระบบ ทั้งที่คอมเมนต์ในไฟล์ระบุว่า "public ส่ง donation ได้"
  // token ที่ "ผิดจริง ๆ" (ไม่ว่าง แต่ไม่ตรงทั้งสองค่า) ยังถูกปฏิเสธเหมือนเดิม
  const isListener = !!LISTEN_TOKEN && tok === LISTEN_TOKEN;
  const isAdmin = !RELAY_TOKEN || (!!tok && tok === RELAY_TOKEN);
  if (tok && !isListener && !isAdmin && (RELAY_TOKEN || LISTEN_TOKEN)) {
    ws.close(4001, 'bad token');
    return;
  }
  ws._scope = isListener ? 'listener' : (isAdmin ? 'admin' : 'public');

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
    try {
      const out = (ws._scope === 'listener' && msg.t === 'donation' && msg.d && msg.d.slip)
        ? { ...msg, d: { ...msg.d, slip: null } } : msg;
      ws.send(JSON.stringify(out));
    } catch (e) {}
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
    if (!allow(ws)) {
      if ((ws._strikes = (ws._strikes || 0) + 1) > 50) { try { ws.close(4008, 'rate limited'); } catch (e) {} }
      return;
    }
    let parsed;
    try { parsed = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!parsed || !parsed.t) return;

    if (ws._scope === 'listener') return; // read-only: ทุกอย่างที่ listener ส่งมาถูกทิ้ง
    const msg = sanitize(parsed);
    if (!msg) return;

    // Capability scope enforcement — no-op today while RELAY_TOKEN is unset (see above).
    if (ADMIN_TYPES.has(msg.t) && ws._scope !== 'admin') return; // drop เงียบ ไม่ relay ไม่ backlog
    if (!ADMIN_TYPES.has(msg.t) && !PUBLIC_TYPES.has(msg.t)) return;

    lastSeen.set(ch, msg._at);

    // Track pending donations in the backlog; drop them once approved/rejected/deleted.
    if (msg.t === 'donation' && msg.d && msg.d.rec && msg.d.rec.id) {
      // บัค (แก้แล้ว): เดิม .set() ทับ id เดิมได้ — ผู้บริจาคคนที่สองส่ง id ซ้ำกับรายการที่ยังรออนุมัติ
      // จะเขียนทับ name/amount/q ของคนแรกใน backlog แอดมินกดอนุมัติรายการจริง (เช่น 500฿)
      // แต่ 'resolved' จะแนบ q/amount ของคนร้าย (เช่น 1฿ + UID ของเขา) ไปให้ bridge สร้างคิวผิดคน
      // ตอนนี้ id ที่มีอยู่แล้วใน backlog ห้ามทับ (รายการแรกชนะ) และไม่ relay รายการซ้ำต่อ
      if (getBacklog(ch).has(msg.d.rec.id)) return;
      getBacklog(ch).set(msg.d.rec.id, msg);
    } else if (msg.t === 'resolved' && msg.d && msg.d.id) {
      // แนบ q + amount จากรายการที่เก็บไว้ (เชื่อถือได้กว่าที่ client ส่งมา) ให้ bridge ใช้สร้างคิว
      // ทำเฉพาะ approved; ค่าจาก backlog เท่านั้น ไม่ใช้ค่าที่ admin client อ้างมาเอง
      const held = getBacklog(ch).get(msg.d.id);
      if (msg.d.status === 'approved' && held && held.d && held.d.rec) {
        if (held.d.rec.q) msg.d.q = held.d.rec.q;
        msg.d.amount = held.d.rec.amount;
      }
      getBacklog(ch).delete(msg.d.id);
    } else if (msg.t === 'alert' && msg.d) {
      // เก็บ alert ล่าสุดไว้สั้น ๆ เผื่อ overlay หลุดต่อกลับพอดีจังหวะที่ยิง alert นี้
      pruneAlerts(ch);
      if (!alertBacklog.has(ch)) alertBacklog.set(ch, []);
      const arr = alertBacklog.get(ch);
      arr.push(msg);
      if (arr.length > ALERT_MAX) arr.shift();
    } else if (msg.t === 'cfg') {
      cfgStore.set(ch, msg);
    }

    // Relay to every OTHER client in the same room. Stringify once (S-05: avoid
    // re-serializing the same message once per peer in a hot broadcast loop).
    const frame = JSON.stringify(msg);
    // listener ไม่ต้องได้รูปสลิป (ใหญ่ + ข้อมูลอ่อนไหว) -> สร้างเฟรมแบบไม่มี slip แยกไว้ครั้งเดียว
    let frameNoSlip = frame;
    if (msg.t === 'donation' && msg.d && msg.d.slip) {
      frameNoSlip = JSON.stringify({ ...msg, d: { ...msg.d, slip: null } });
    }
    const peers = getRoom(ch);
    for (const client of peers) {
      if (client !== ws && client.readyState === client.OPEN) {
        try { client.send(client._scope === 'listener' ? frameNoSlip : frame); } catch (e) {}
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

// บัค (แก้แล้ว): เดิมไม่มี SIGTERM handler — Render ส่ง SIGTERM ทุกครั้งที่ deploy ใหม่/สเกล
// ลง ทำให้ socket ทุกตัวหลุดแบบ abnormal close (1006) พร้อมกันหมด แทนที่จะได้ close frame
// ปกติ (1001 "going away") ซึ่งฝั่ง client จะ reconnect พร้อมกันทันทีตอนที่ตัวเองยังบูตไม่เสร็จ
function shutdown() {
  clearInterval(interval);
  clearInterval(gcTimer);
  wss.clients.forEach((c) => { try { c.close(1001, 'server restarting'); } catch (e) {} });
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

if (LISTEN_TOKEN && LISTEN_TOKEN === RELAY_TOKEN) {
  console.error('!! LISTEN_TOKEN ต้องไม่เหมือน RELAY_TOKEN — ไม่งั้น listener จะมีสิทธิ์อ่านเท่านั้นไม่ได้จริง (ทั้งคู่ตรงกัน = admin)');
}
if (!RELAY_TOKEN) {
  console.warn('==================================================================');
  console.warn('  RELAY_TOKEN ยังไม่ได้ตั้งค่า — ทุก connection ได้สิทธิ์ admin เท่ากันหมด');
  console.warn('  (ใครที่รู้ channel key ก็ยังส่ง alert/cfg ปลอมได้เหมือนเดิม)');
  console.warn('  ตั้ง RELAY_TOKEN บน Render + CFG.token ในแอดมินเพื่อปิดช่องนี้จริง');
  console.warn('==================================================================');
}

server.listen(PORT, () => {
  console.log(`RZ relay listening on :${PORT} (ws path /ws)`);
});
