// ════════════════════════════════════════════════════════════════════
//  tts.js — Server-side TTS proxy (Edge Read Aloud -> Google Translate fallback)
//  ไม่มี dependency ใหม่ — ใช้ ws + fetch ที่มีอยู่แล้ว (Node >=18 มี global fetch)
//
//  ทำไมต้องมีไฟล์นี้ (F-TTS-01): OBS Browser Source รันบน CEF (Chromium Embedded
//  Framework) ซึ่งไม่ได้ผูกกับ speech engine ของ OS ทำให้ speechSynthesis.getVoices()
//  คืนค่า array ว่างเปล่าเสมอ ไม่ใช่ race condition ไม่ใช่ permission ปัญหา — คือ "ไม่มี
//  เสียงให้ใช้ตั้งแต่แรก" เมื่อรันจริงในสตรีม ทางแก้เดียวที่ใช้ได้จริง (เหมือน StreamElements/
//  StreamLabs) คือสังเคราะห์เสียงที่ฝั่งเซิร์ฟเวอร์แล้วส่ง MP3 กลับมาให้ overlay เล่นผ่าน <audio>
// ════════════════════════════════════════════════════════════════════
'use strict';
const crypto    = require('crypto');
const WebSocket = require('ws');

// ── Edge Read Aloud (unofficial MS consumer endpoint — no official support,
// can 403 if MS changes auth; that's why there's a Google Translate fallback below) ──
const TRUSTED  = '6A5AA1D4EAFF4E9FB37E23D68491D6F4';
const WSS      = 'wss://speech.platform.bing.com/consumer/speech/' +
                 'synthesize/readaloud/edge/v1';
const CHROMIUM = '130.0.2849.68';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
           '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0';
const HEADERS = {
  'User-Agent'     : UA,
  'Origin'         : 'chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold',
  'Accept-Language': 'en-US,en;q=0.9',
  'Cache-Control'  : 'no-cache',
  'Pragma'         : 'no-cache'
};

const VOICES = {
  f : 'th-TH-PremwadeeNeural',   // หญิง — ดีฟอลต์ ฟังสบายสุดสำหรับโดเนท
  m : 'th-TH-NiwatNeural',       // ชาย
  f2: 'th-TH-AcharaNeural',      // หญิง โทนอีกแบบ
  en: 'en-US-AriaNeural'
};

// ── Sec-MS-GEC token ──────────────────────────────────────────────────
// ⚠ ticks เกิน Number.MAX_SAFE_INTEGER (~1.34e17 vs 9.0e15) ต้องใช้ BigInt เท่านั้น
// ไม่งั้นจะได้ hash ผิด (precision loss) → เจอ 403 ตลอด
function secMsGec() {
  let sec = BigInt(Math.floor(Date.now() / 1000) + 11644473600);
  sec = (sec / 300n) * 300n;                    // ปัดลงทีละ 5 นาที
  const ticks = sec * 10000000n;                // วินาที → 100-nanosecond
  return crypto.createHash('sha256')
               .update(ticks.toString() + TRUSTED, 'ascii')
               .digest('hex').toUpperCase();
}

// ⚠ จุดฉีด SSML — ข้อความโดเนทถูกยัดลง XML ตรงๆ ถ้ามีคนใส่ </voice><voice name=...>
// จะเปลี่ยนเสียง/ความเร็ว หรือทำให้ request พังได้ ต้อง escape ทุกครั้ง
const xmlEsc = (s) => String(s)
  .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '')
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&apos;');

function edgeTTS(text, voice, rate) {
  return new Promise((resolve, reject) => {
    const id  = crypto.randomBytes(16).toString('hex').toUpperCase();
    const url = `${WSS}?TrustedClientToken=${TRUSTED}` +
                `&Sec-MS-GEC=${secMsGec()}` +
                `&Sec-MS-GEC-Version=1-${CHROMIUM}` +
                `&ConnectionId=${id}`;
    const sock   = new WebSocket(url, { headers: HEADERS, handshakeTimeout: 8000 });
    const chunks = [];
    let settled  = false;

    const fin = (err, buf) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { sock.close(); } catch (_) {}
      err ? reject(err) : resolve(buf);
    };
    const timer = setTimeout(() => fin(new Error('edge:timeout')), 15000);

    sock.on('open', () => {
      const ts = new Date().toString();
      sock.send(
        `X-Timestamp:${ts}\r\n` +
        'Content-Type:application/json; charset=utf-8\r\n' +
        'Path:speech.config\r\n\r\n' +
        '{"context":{"synthesis":{"audio":{"metadataoptions":{' +
        '"sentenceBoundaryEnabled":"false","wordBoundaryEnabled":"false"},' +
        '"outputFormat":"audio-24khz-48kbitrate-mono-mp3"}}}}'
      );
      sock.send(
        `X-RequestId:${id}\r\n` +
        'Content-Type:application/ssml+xml\r\n' +
        `X-Timestamp:${ts}Z\r\n` +
        'Path:ssml\r\n\r\n' +
        `<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' ` +
        `xml:lang='th-TH'><voice name='${voice}'>` +
        `<prosody rate='${rate}' pitch='+0Hz'>${xmlEsc(text)}</prosody>` +
        `</voice></speak>`
      );
    });

    sock.on('message', (data, isBinary) => {
      if (isBinary) {
        const hLen = data.readUInt16BE(0);
        const head = data.subarray(2, 2 + hLen).toString('utf8');
        if (head.includes('Path:audio')) chunks.push(data.subarray(2 + hLen));
      } else if (data.toString().includes('Path:turn.end')) {
        chunks.length ? fin(null, Buffer.concat(chunks))
                      : fin(new Error('edge:empty'));
      }
    });
    sock.on('error', (e) => fin(new Error('edge:' + (e.message || 'ws'))));
    sock.on('close', () => fin(new Error('edge:closed')));
  });
}

// ── Fallback: Google Translate TTS ──────────────────────────────────
// จำกัด ~200 ตัวอักษร/คำขอ → ต้องหั่น แล้วต่อ MP3 frame เข้าด้วยกัน
function chunkText(s, n) {
  const out = [];
  let cur = '';
  for (const w of String(s).split(/(\s+)/)) {
    if ((cur + w).length > n) {
      if (cur) out.push(cur);
      cur = '';
      for (let i = 0; i < w.length; i += n) out.push(w.slice(i, i + n));
      continue;
    }
    cur += w;
  }
  if (cur.trim()) out.push(cur);
  return out.filter((x) => x.trim());
}

async function googleTTS(text, lang) {
  const bufs = [];
  for (const part of chunkText(text, 190)) {
    const u = 'https://translate.google.com/translate_tts?ie=UTF-8' +
              '&client=tw-ob&ttsspeed=1&tl=' + encodeURIComponent(lang) +
              '&q=' + encodeURIComponent(part);
    const r = await fetch(u, {
      headers: { 'User-Agent': UA, Referer: 'https://translate.google.com/' },
      signal : AbortSignal.timeout(10000)
    });
    if (!r.ok) throw new Error('gtts:' + r.status);
    bufs.push(Buffer.from(await r.arrayBuffer()));
  }
  if (!bufs.length) throw new Error('gtts:empty');
  return Buffer.concat(bufs);
}

// ── LRU cache ──────────────────────────────────────────────────────
const CACHE = new Map();
const CACHE_MAX   = 250;
const CACHE_BYTES = 48 * 1024 * 1024;
let   cacheBytes  = 0;

function cacheGet(k) {
  const v = CACHE.get(k);
  if (!v) return null;
  CACHE.delete(k); CACHE.set(k, v);          // touch → ย้ายไปท้ายแถว
  return v;
}
function cacheSet(k, buf) {
  if (buf.length > 4 * 1024 * 1024) return;
  CACHE.set(k, buf);
  cacheBytes += buf.length;
  while (CACHE.size > CACHE_MAX || cacheBytes > CACHE_BYTES) {
    const oldest = CACHE.keys().next().value;
    if (oldest === undefined) break;
    cacheBytes -= CACHE.get(oldest).length;
    CACHE.delete(oldest);
  }
}

// ── rate limit ต่อ IP ──────────────────────────────────────────────
const buckets = new Map();
function rateOk(ip) {
  const now = Date.now();
  let b = buckets.get(ip);
  if (!b) { b = { tok: 12, last: now }; buckets.set(ip, b); }
  b.tok = Math.min(12, b.tok + ((now - b.last) / 1000) * 0.5);  // ~30 ครั้ง/นาที
  b.last = now;
  if (b.tok < 1) return false;
  b.tok -= 1;
  return true;
}
const bucketGc = setInterval(() => {
  const cut = Date.now() - 600000;
  for (const [k, v] of buckets) if (v.last < cut) buckets.delete(k);
}, 300000);
bucketGc.unref();

// ── ทำความสะอาดข้อความก่อนอ่าน ─────────────────────────────────────
function prep(raw) {
  const t = String(raw || '')
    .replace(/https?:\/\/\S+/gi, ' ลิงก์ ')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}]/gu, ' ')
    .replace(/(.)\1{4,}/gu, '$1$1$1')          // "555555555" → "555"
    .replace(/\s+/g, ' ')
    .trim();
  return t.slice(0, 300);
}

let lastEdgeOk = null; // for /tts/health

// ── HTTP handler ───────────────────────────────────────────────────
async function handleTTS(req, res) {
  try {
    const u  = new URL(req.url, 'http://x');
    const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
               || req.socket.remoteAddress || 'ip';

    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin' : '*',
        'Access-Control-Allow-Headers': 'content-type',
        'Access-Control-Max-Age'      : '86400'
      });
      return res.end();
    }
    if (req.method !== 'GET') { res.writeHead(405); return res.end(); }
    if (!rateOk(ip)) {
      res.writeHead(429, { 'Retry-After': '5' });
      return res.end('slow down');
    }

    const text = prep(u.searchParams.get('q'));
    if (!text) { res.writeHead(400); return res.end('empty'); }

    const vKey  = u.searchParams.get('v') || 'f';
    const voice = VOICES[vKey] || VOICES.f;
    const rNum  = Math.max(-40, Math.min(60, parseInt(u.searchParams.get('r'), 10) || 0));
    const rate  = (rNum >= 0 ? '+' : '') + rNum + '%';
    const key   = crypto.createHash('sha1')
                        .update(voice + '|' + rate + '|' + text).digest('hex');

    const send = (buf, src) => {
      res.writeHead(200, {
        'Content-Type'                : 'audio/mpeg',
        'Content-Length'              : buf.length,
        'Cache-Control'               : 'public, max-age=86400, immutable',
        'Access-Control-Allow-Origin' : '*',
        'X-TTS-Source'                : src        // ดูใน DevTools ได้ว่ามาจากไหน
      });
      res.end(buf);
    };

    const hit = cacheGet(key);
    if (hit) return send(hit, 'cache');

    let buf = null, err1 = null;
    try {
      buf = await edgeTTS(text, voice, rate);
      lastEdgeOk = true;
    } catch (e) {
      err1 = e;
      lastEdgeOk = false;
      console.warn('[tts] edge fail ->', e.message, '| fallback google');
      try {
        buf = await googleTTS(text, vKey === 'en' ? 'en' : 'th');
      } catch (e2) {
        console.error('[tts] all providers failed', err1.message, e2.message);
        res.writeHead(503, { 'Access-Control-Allow-Origin': '*' });
        return res.end('tts unavailable');
      }
    }
    cacheSet(key, buf);
    send(buf, err1 ? 'google' : 'edge');
  } catch (e) {
    console.error('[tts] handler', e);
    if (!res.headersSent) res.writeHead(500, { 'Access-Control-Allow-Origin': '*' });
    res.end('err');
  }
}

// เช็คก่อนขึ้นไลฟ์ว่า Edge TTS ยังใช้ได้อยู่ไหม (ไม่ได้ synth จริง แค่รายงานผลครั้งล่าสุด
// + true-up แบบ lightweight ถ้ายังไม่เคยเรียกเลย)
async function handleTTSHealth(req, res) {
  try {
    if (lastEdgeOk === null) {
      try { await edgeTTS('ทดสอบ', VOICES.f, '+0%'); lastEdgeOk = true; }
      catch (e) { lastEdgeOk = false; }
    }
    res.writeHead(200, { 'content-type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, edge: lastEdgeOk, cacheEntries: CACHE.size }));
  } catch (e) {
    res.writeHead(500, { 'content-type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: false }));
  }
}

module.exports = { handleTTS, handleTTSHealth, VOICES, prep };
