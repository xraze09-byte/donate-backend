// ════════════════════════════════════════════════════════════════════
//  tts.js  v3  — แทนที่ไฟล์เดิมทั้งไฟล์
//  แก้ครบทั้ง 4 ตัว + เพิ่ม circuit breaker + /tts/diag
// ════════════════════════════════════════════════════════════════════
'use strict';
const crypto    = require('crypto');
const WebSocket = require('ws');

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

// ── BUG-1 FIX : เหลือเฉพาะเสียงที่ "มีจริง" ใน Edge TTS ─────────────
//    ลบ f2 / AcharaNeural ทิ้ง (เป็นเสียง Azure ไม่ใช่ Edge)
const VOICES = {
  f : 'th-TH-PremwadeeNeural',   // หญิงไทย
  m : 'th-TH-NiwatNeural',       // ชายไทย
  en: 'en-US-AriaNeural'         // ใช้เฉพาะข้อความที่ไม่มีอักษรไทยเลย
};

// ── BUG-2 FIX : ตรวจ script แล้วบังคับเสียงให้ตรงเสมอ ───────────────
const TH_CHAR = /[\u0E00-\u0E7F]/;
const LATIN   = /[A-Za-z]/;

function pickVoice(text, want) {
  const hasTh = TH_CHAR.test(text);
  if (hasTh) {
    // มีอักษรไทย → ต้องใช้เสียงไทยเท่านั้น ไม่ว่า client ขออะไรมา
    return (want === 'm') ? VOICES.m : VOICES.f;
  }
  if (LATIN.test(text)) return VOICES.en;
  // ตัวเลข/สัญลักษณ์ล้วน — เสียงไทยอ่านตัวเลขได้ดีกว่า
  return (want === 'm') ? VOICES.m : VOICES.f;
}

// ── BUG-2 FIX (ระดับคุณภาพ) : หั่นตาม script ─────────────────────────
//    run สั้นกว่า 4 ตัวจะถูกยุบรวมกับก้อนก่อนหน้า กันเสียงกระตุก
function splitByScript(text) {
  const runs = [];
  let cur = null;
  for (const ch of text) {
    const k = TH_CHAR.test(ch) ? 'th' : (LATIN.test(ch) ? 'en' : 'x');
    if (!cur)                       { cur = { k, s: ch }; continue; }
    if (k === 'x' || k === cur.k)   { cur.s += ch;        continue; }
    if (cur.k === 'x')              { cur.k = k; cur.s += ch; continue; }
    runs.push(cur); cur = { k, s: ch };
  }
  if (cur) runs.push(cur);

  const out = [];
  for (const r of runs) {
    const last = out[out.length - 1];
    if (last && r.s.trim().length < 4) { last.s += r.s; continue; }
    out.push(r);
  }
  return out.filter(r => r.s.trim());
}

// ── BUG-4 FIX : X-Timestamp ต้องเป็น UTC เสมอ ───────────────────────
//    new Date().toString() จะใช้ timezone ของเครื่อง ถ้าเทสต์ที่ไทยจะได้
//    "GMT+0700 (Indochina Time)" ซึ่งไม่ตรงรูปแบบที่ฝั่งนั้นคาดหวัง
const DY = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const MO = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function edgeDate() {
  const d = new Date(), p = n => String(n).padStart(2, '0');
  return `${DY[d.getUTCDay()]} ${MO[d.getUTCMonth()]} ${p(d.getUTCDate())} ` +
         `${d.getUTCFullYear()} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:` +
         `${p(d.getUTCSeconds())} GMT+0000 (Coordinated Universal Time)`;
}

function secMsGec() {
  let sec = BigInt(Math.floor(Date.now() / 1000) + 11644473600);
  sec = (sec / 300n) * 300n;
  return crypto.createHash('sha256')
               .update((sec * 10000000n).toString() + TRUSTED, 'ascii')
               .digest('hex').toUpperCase();
}

const xmlEsc = s => String(s)
  .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '')
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&apos;');

// ── circuit breaker : ถ้า Edge โดน 403 ก็อย่าเสียเวลาลองทุกครั้ง ────
const brk = { fails: 0, until: 0, lastErr: '' };
const edgeUsable = () => Date.now() > brk.until;

function edgeTTS(text, voice, rate) {
  return new Promise((resolve, reject) => {
    const id  = crypto.randomBytes(16).toString('hex').toUpperCase();
    const url = `${WSS}?TrustedClientToken=${TRUSTED}` +
                `&Sec-MS-GEC=${secMsGec()}` +
                `&Sec-MS-GEC-Version=1-${CHROMIUM}&ConnectionId=${id}`;
    const sock   = new WebSocket(url, { headers: HEADERS, handshakeTimeout: 8000 });
    const chunks = [];
    let settled = false, bytes = 0;

    const fin = (err, buf) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { sock.close(); } catch (_) {}
      err ? reject(err) : resolve(buf);
    };
    const timer = setTimeout(() => fin(new Error('edge:timeout')), 15000);

    // ── BUG-4 FIX : ดัก HTTP error ตอน handshake ให้ชัด ───────────
    sock.on('unexpected-response', (_req, res) => {
      res.resume();
      fin(new Error('edge:http' + res.statusCode +
                    (res.statusCode === 403 ? ' (datacenter IP ถูกบล็อก)' : '')));
    });

    sock.on('open', () => {
      const ts = edgeDate();
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
        if (data.subarray(2, 2 + hLen).toString('utf8').includes('Path:audio')) {
          const c = data.subarray(2 + hLen);
          if (c.length) { chunks.push(c); bytes += c.length; }
        }
      } else if (data.toString().includes('Path:turn.end')) {
        // ── BUG-2 FIX : นับ "ไบต์" ไม่ใช่ "จำนวน chunk" ────────────
        //    voice ผิดภาษาจะส่ง chunk ว่างมาให้ ทำให้ length > 0 แต่ 0 ไบต์
        if (bytes < 512) return fin(new Error('edge:empty (voice/ภาษาไม่ตรงกัน)'));
        fin(null, Buffer.concat(chunks));
      }
    });
    sock.on('error', e  => fin(new Error('edge:' + (e.message || 'ws'))));
    sock.on('close', () => fin(new Error('edge:closed')));
  });
}

// ── Google Translate TTS (fallback) ─────────────────────────────────
// BUG-3 FIX : ตรวจ content-type + magic bytes กัน captcha HTML
const THAI_COMB = /[\u0E31\u0E34-\u0E3A\u0E47-\u0E4E]/;

// ภาษาไทยไม่มีช่องว่าง ตัดมั่วจะได้เสียงเพี้ยน
// ถอยจุดตัดจนกว่าจะไม่ตกกลางสระลอย/วรรณยุกต์
function thChunks(s, n) {
  const out = [];
  let rest = String(s);
  while (rest.length > n) {
    let i = n;
    const sp = rest.lastIndexOf(' ', n);
    if (sp > n * 0.6) i = sp;                       // ตัดที่ช่องว่างถ้าใกล้พอ
    while (i > 1 && THAI_COMB.test(rest[i])) i--;   // ไม่ตัดกลางสระ/วรรณยุกต์
    out.push(rest.slice(0, i));
    rest = rest.slice(i);
  }
  if (rest.trim()) out.push(rest);
  return out.filter(x => x.trim());
}

function isMp3(b) {
  return b.length > 512 &&
         ((b[0] === 0xFF && (b[1] & 0xE0) === 0xE0) ||          // MPEG frame
          (b[0] === 0x49 && b[1] === 0x44 && b[2] === 0x33));   // ID3
}

async function googleTTS(text, lang) {
  const bufs = [];
  for (const part of thChunks(text, 185)) {
    const u = 'https://translate.google.com/translate_tts?ie=UTF-8' +
              '&client=tw-ob&ttsspeed=1&tl=' + encodeURIComponent(lang) +
              '&q=' + encodeURIComponent(part);
    const r = await fetch(u, {
      headers: { 'User-Agent': UA, 'Referer': 'https://translate.google.com/',
                 'Accept': 'audio/mpeg,*/*' },
      signal : AbortSignal.timeout(10000)
    });
    if (!r.ok) throw new Error('gtts:' + r.status);
    const ct = (r.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('audio')) throw new Error('gtts:notaudio(' + ct + ')');
    const b = Buffer.from(await r.arrayBuffer());
    if (!isMp3(b)) throw new Error('gtts:captcha');   // ได้ HTML มาแทน MP3
    bufs.push(b);
  }
  if (!bufs.length) throw new Error('gtts:empty');
  return Buffer.concat(bufs);
}

// ── ตัวรวม : หั่นตาม script → สังเคราะห์ทีละก้อน → ต่อ MP3 ──────────
async function synth(text, want, rate) {
  const segs = splitByScript(text);
  const bufs = [];
  let usedGoogle = false;

  for (const seg of segs) {
    const voice = pickVoice(seg.s, want);
    let buf = null;

    if (edgeUsable()) {
      try {
        buf = await edgeTTS(seg.s, voice, rate);
        brk.fails = 0;
      } catch (e) {
        brk.lastErr = e.message;
        if (++brk.fails >= 3) {
          brk.until = Date.now() + 10 * 60 * 1000;
          brk.fails = 0;
          console.warn('[tts] edge circuit OPEN 10 นาที —', e.message);
        } else {
          console.warn('[tts] edge fail →', e.message);
        }
      }
    }
    if (!buf) {
      buf = await googleTTS(seg.s, seg.k === 'en' ? 'en' : 'th');
      usedGoogle = true;
    }
    bufs.push(buf);
  }
  if (!bufs.length) throw new Error('synth:empty');
  return { buf: Buffer.concat(bufs), src: usedGoogle ? 'google' : 'edge' };
}

// ── cache / rate limit / prep : เหมือนเดิม แต่เพิ่มการกันไฟล์เสีย ────
const CACHE = new Map();
const CACHE_MAX = 250, CACHE_BYTES = 48 * 1024 * 1024;
let cacheBytes = 0;

function cacheGet(k) {
  const v = CACHE.get(k);
  if (!v) return null;
  CACHE.delete(k); CACHE.set(k, v);
  return v;
}
function cacheSet(k, buf) {
  if (!isMp3(buf) || buf.length > 4 * 1024 * 1024) return;   // ห้าม cache ของเสีย
  CACHE.set(k, buf);
  cacheBytes += buf.length;
  while (CACHE.size > CACHE_MAX || cacheBytes > CACHE_BYTES) {
    const o = CACHE.keys().next().value;
    if (o === undefined) break;
    cacheBytes -= CACHE.get(o).length;
    CACHE.delete(o);
  }
}

const buckets = new Map();
function rateOk(ip) {
  const now = Date.now();
  let b = buckets.get(ip);
  if (!b) { b = { tok: 12, last: now }; buckets.set(ip, b); }
  b.tok = Math.min(12, b.tok + (now - b.last) / 1000 * 0.5);
  b.last = now;
  if (b.tok < 1) return false;
  b.tok -= 1;
  return true;
}
setInterval(() => {
  const cut = Date.now() - 600000;
  for (const [k, v] of buckets) if (v.last < cut) buckets.delete(k);
}, 300000).unref();

function prep(raw) {
  return String(raw || '')
    .replace(/https?:\/\/\S+/gi, ' ลิงก์ ')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE0F}]/gu, ' ')
    .replace(/(.)\1{4,}/gu, '$1$1$1')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 300);
}

// ── HTTP handler ────────────────────────────────────────────────────
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
    if (u.pathname === '/tts/diag') return handleDiag(res);
    if (req.method !== 'GET')  { res.writeHead(405); return res.end(); }
    if (!rateOk(ip)) { res.writeHead(429, { 'Retry-After': '5' }); return res.end('slow'); }

    const text = prep(u.searchParams.get('q'));
    if (!text) { res.writeHead(400); return res.end('empty'); }

    const want = u.searchParams.get('v') || 'auto';
    const rNum = Math.max(-40, Math.min(60, parseInt(u.searchParams.get('r'), 10) || 0));
    const rate = (rNum >= 0 ? '+' : '') + rNum + '%';
    const key  = crypto.createHash('sha1').update(want + '|' + rate + '|' + text)
                       .digest('hex');

    const send = (buf, src) => res.writeHead(200, {
        'Content-Type'               : 'audio/mpeg',
        'Content-Length'             : buf.length,
        'Cache-Control'              : 'public, max-age=86400, immutable',
        'Access-Control-Allow-Origin': '*',
        'X-TTS-Source'               : src,
        'X-TTS-Voice'                : pickVoice(text, want)   // ดีบักจาก DevTools
      }) || res.end(buf);

    const hit = cacheGet(key);
    if (hit) return send(hit, 'cache');

    let out;
    try {
      out = await synth(text, want, rate);
    } catch (e) {
      console.error('[tts] ทุก provider ล้มเหลว:', e.message, '| edge:', brk.lastErr);
      res.writeHead(503, {
        'Access-Control-Allow-Origin': '*',
        'X-TTS-Error': String(e.message).slice(0, 120)
      });
      return res.end('tts unavailable');
    }
    cacheSet(key, out.buf);
    send(out.buf, out.src);
  } catch (e) {
    console.error('[tts] handler', e);
    if (!res.headersSent) res.writeHead(500, { 'Access-Control-Allow-Origin': '*' });
    res.end('err');
  }
}

// ── /tts/diag : บอกทันทีว่า provider ไหนใช้ได้ ไม่ต้องเดา ────────────
async function handleDiag(res) {
  const t = 'ทดสอบเสียงภาษาไทย หนึ่ง สอง สาม';
  const out = {
    node   : process.version,
    breaker: brk.until > Date.now()
               ? ('OPEN อีก ' + Math.ceil((brk.until - Date.now()) / 1000) + ' วิ')
               : 'closed',
    lastEdgeError: brk.lastErr || null
  };
  for (const [name, fn] of [
    ['edge_female', () => edgeTTS(t, VOICES.f, '+0%')],
    ['edge_male',   () => edgeTTS(t, VOICES.m, '+0%')],
    ['edge_wrong',  () => edgeTTS(t, VOICES.en, '+0%')],   // ต้อง FAIL = ถูกต้อง
    ['google_th',   () => googleTTS(t, 'th')]
  ]) {
    try { const b = await fn(); out[name] = 'ok ' + b.length + ' bytes'; }
    catch (e) { out[name] = 'FAIL ' + e.message; }
  }
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(JSON.stringify(out, null, 2));
}


// เช็คก่อนขึ้นไลฟ์ว่า Edge TTS ยังใช้ได้อยู่ไหม (lightweight, ไม่บังคับ synth ทุกครั้ง)
let lastEdgeOk = null;
async function handleTTSHealth(req, res) {
  try {
    if (lastEdgeOk === null) {
      try { await edgeTTS('ทดสอบ', VOICES.f, '+0%'); lastEdgeOk = true; }
      catch (e) { lastEdgeOk = false; }
    }
    res.writeHead(200, { 'content-type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, edge: lastEdgeOk, breakerOpen: Date.now() < brk.until, cacheEntries: CACHE.size }));
  } catch (e) {
    res.writeHead(500, { 'content-type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: false }));
  }
}

module.exports = { handleTTS, handleTTSHealth, VOICES, prep, pickVoice, splitByScript };