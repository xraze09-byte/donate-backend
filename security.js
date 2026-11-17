'use strict';
// security.js — opt-in hardening for the donation relay. Every feature is OFF by default, so
// deploying this file changes nothing until the matching env var is set. Each one also has a
// LOG-ONLY mode ("log") so you can watch what WOULD be rejected on real traffic before enforcing.
//
//   REQUIRE_SIGNED = 0 | log | 1   alert/cfg/resolved must carry a well-formed sig+nonce+ts
//   CSRF_MODE      = 0 | log | 1   donation must carry a valid short-lived CSRF token (GET /csrf)
//   POW_BITS       = 0..24         donation must carry a proof-of-work with >= this many leading zero bits
//   POW_MODE       = log | 1       (default 1 when POW_BITS > 0) log = count failures but let them through
//   STRICT_DONATION= 0 | log | 1   extra shape checks on the donation record
//   CSRF_SECRET    = long random   HMAC key for CSRF tokens (unset: random per boot, tokens die on restart)
//
// Nothing here can verify an admin signature: the relay holds no key, and must not. Overlay and
// donor page verify signatures themselves. REQUIRE_SIGNED only rejects messages that are not even
// shaped like a signed message, which stops casual forgery of alert/cfg/resolved.

const crypto = require('crypto');

const mode = (v) => (v === '1' ? 'on' : v === 'log' ? 'log' : 'off');
const CFG = {
  signed: mode(process.env.REQUIRE_SIGNED),
  csrf: mode(process.env.CSRF_MODE),
  strict: mode(process.env.STRICT_DONATION),
  powBits: Math.min(24, Math.max(0, parseInt(process.env.POW_BITS, 10) || 0)),
  powMode: process.env.POW_MODE === 'log' ? 'log' : 'on',
  csrfMaxAgeMs: 15 * 60 * 1000,
};
const SECRET = process.env.CSRF_SECRET || crypto.randomBytes(32).toString('hex');
const stats = { csrf: 0, pow: 0, strict: 0, signed: 0 };   // would-reject / rejected counters (shown on /health)

// ---- canonical JSON: must match the browser's canon() byte for byte -----------------------------
// keys sorted, undefined dropped, and the transport-only fields never part of what is signed/hashed.
const SKIP = new Set(['sig', 'tok', 'csrf', 'pow']);
function canon(v) {
  if (Array.isArray(v)) return '[' + v.map(canon).join(',') + ']';
  if (v && typeof v === 'object') {
    return '{' + Object.keys(v).filter((k) => !SKIP.has(k) && v[k] !== undefined).sort()
      .map((k) => JSON.stringify(k) + ':' + canon(v[k])).join(',') + '}';
  }
  const s = JSON.stringify(v);
  return s === undefined ? 'null' : s;
}

// ---- CSRF: stateless HMAC token bound to the caller's IP ---------------------------------------
const mac = (s) => crypto.createHmac('sha256', SECRET).update(s).digest();
function issueCsrf(ip) {
  const ts = Date.now().toString(36), n = crypto.randomBytes(9).toString('base64url');
  return ts + '.' + n + '.' + mac(ip + '|' + ts + '|' + n).toString('base64url');
}
function checkCsrf(tok, ip) {
  if (typeof tok !== 'string' || tok.length > 300) return false;
  const [ts, n, m] = tok.split('.');
  if (!ts || !n || !m) return false;
  const age = Date.now() - parseInt(ts, 36);
  if (!(age >= 0 && age < CFG.csrfMaxAgeMs)) return false;
  const a = Buffer.from(m, 'base64url'), b = mac(ip + '|' + ts + '|' + n);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ---- proof-of-work -----------------------------------------------------------------------------
function leadingZeroBits(buf) {
  let z = 0;
  for (const x of buf) { if (x) { z += Math.clz32(x) - 24; break; } z += 8; }
  return z;
}
// What the work is bound to: the whole donation payload (so it can't be reused for another donation),
// with the slip image replaced by its hash so the browser doesn't have to hash megabytes per attempt.
function powPayload(d) {
  const c = Object.assign({}, d);
  if (typeof c.slip === 'string') c.slip = crypto.createHash('sha256').update(c.slip).digest('base64url');
  return canon(c);
}
function checkPow(d, pow, bits) {
  if (!(bits > 0)) return true;
  if (!pow || !Number.isInteger(pow.n) || pow.n < 0 || pow.n > 1e9 || !(pow.bits >= bits)) return false;
  const h = crypto.createHash('sha256').update(powPayload(d) + '|' + pow.n).digest();
  return leadingZeroBits(h) >= bits;
}

// ---- strict donation shape ---------------------------------------------------------------------
// Runs on the already-sanitised record (server.js sanitizeRec), so it only adds what that does not.
const BADC = /[\u0000-\u0008\u000B-\u001F\u007F-\u009F\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]/;
const LINK = /\b(?:https?:|javascript:|data:|vbscript:|www\.)/i;
function strictDonation(rec, opts) {
  const o = opts || {};
  if (!rec || typeof rec !== 'object') return 'shape';
  if (!/^[\w.-]{6,64}$/.test(String(rec.id))) return 'id';
  const a = rec.amount;
  if (typeof a !== 'number' || !Number.isFinite(a) || a <= 0) return 'amount';
  if (Math.round(a * 100) !== a * 100 && Math.abs(Math.round(a * 100) - a * 100) > 1e-6) return 'decimals';
  if (o.min && a < o.min) return 'min';
  if (o.max && a > o.max) return 'max';
  if (BADC.test(rec.name || '') || BADC.test(rec.message || '')) return 'control-chars';
  if (LINK.test((rec.name || '') + ' ' + (rec.message || ''))) return 'link';
  if (!(Math.abs(Date.now() - (Number(rec.ts) || 0)) < 15 * 60 * 1000)) return 'ts';
  return null;
}

// ---- signed-shape check for admin messages -----------------------------------------------------
// sanitize() already drops malformed sig/nonce/ts via sigFields(); an admin message with none of
// them comes out with no `sig` key. That is exactly the "unsigned" case.
function isSignedShape(d) {
  return !!(d && typeof d.sig === 'string' && typeof d.nonce === 'string' && Number.isFinite(d.ts) && d.ts > 0);
}

// Apply an enforcement mode. Returns true when the message should be DROPPED.
function enforce(kind, m, failed, log) {
  if (!failed) return false;
  stats[kind]++;
  if (m === 'log') { log('[sec] would reject (' + kind + '): ' + failed); return false; }
  log('[sec] rejected (' + kind + '): ' + failed);
  return true;
}

module.exports = { CFG, stats, canon, issueCsrf, checkCsrf, checkPow, powPayload, strictDonation, isSignedShape, enforce, leadingZeroBits };
