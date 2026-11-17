/* client/donate-security.js — browser side of the relay's opt-in hardening (CSRF token + proof-of-work).
   Drop this into the donate page. It is deliberately dependency-free and works with or without the
   server features enabled: if /csrf is unreachable it returns null and the page keeps working.

   Usage, where the page currently does:   ws.send(JSON.stringify({t:'donation', d:{rec, slip}}))
   do instead:                              const sec = await DonateSecurity.prepare(API, rec, slip);
                                            ws.send(JSON.stringify({t:'donation', d:{rec, slip, ...sec}}))
   `sec` is { csrf?, pow? }. Spread it into d. Never into rec (rec is what the proof is bound to). */
(function (root) {
  'use strict';
  var enc = new TextEncoder();
  var SKIP = { sig: 1, tok: 1, csrf: 1, pow: 1 };
  // MUST match security.js canon() exactly.
  function canon(v) {
    if (Array.isArray(v)) return '[' + v.map(canon).join(',') + ']';
    if (v && typeof v === 'object') {
      return '{' + Object.keys(v).filter(function (k) { return !SKIP[k] && v[k] !== undefined; }).sort()
        .map(function (k) { return JSON.stringify(k) + ':' + canon(v[k]); }).join(',') + '}';
    }
    var s = JSON.stringify(v); return s === undefined ? 'null' : s;
  }
  function b64u(buf) {
    var s = ''; var a = new Uint8Array(buf);
    for (var i = 0; i < a.length; i++) s += String.fromCharCode(a[i]);
    return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }
  function sha(bytes) { return crypto.subtle.digest('SHA-256', bytes).then(function (b) { return new Uint8Array(b); }); }
  function lz(h) { var z = 0; for (var i = 0; i < h.length; i++) { if (h[i]) { z += Math.clz32(h[i]) - 24; break; } z += 8; } return z; }

  // The proof is bound to rec (+ the slip's hash, so we never hash megabytes per attempt).
  async function payload(rec, slip) {
    var bound = Object.assign({}, rec);
    if (typeof slip === 'string') bound.slip = b64u(await sha(enc.encode(slip)));
    return canon(bound);
  }
  async function solve(rec, slip, bits) {
    if (!(bits > 0) || !root.crypto || !crypto.subtle) return null;
    var base = await payload(rec, slip);
    for (var n = 0; n < 4e6; n += 256) {
      var batch = [];
      for (var i = 0; i < 256; i++) batch.push(sha(enc.encode(base + '|' + (n + i))));
      var hs = await Promise.all(batch);
      for (var j = 0; j < 256; j++) if (lz(hs[j]) >= bits) return { n: n + j, bits: bits };
    }
    return null;
  }
  async function csrf(api) {
    try {
      var c = new AbortController(), t = setTimeout(function () { c.abort(); }, 6000);
      var r = await fetch(api.replace(/\/$/, '') + '/csrf', { cache: 'no-store', credentials: 'omit', referrerPolicy: 'no-referrer', signal: c.signal });
      clearTimeout(t);
      if (!r.ok) return null;
      var j = await r.json();
      return j && typeof j.token === 'string' ? j.token : null;
    } catch (e) { return null; }
  }
  // api: relay base URL (https://...). bits: the server's POW_BITS (read it from /health -> sec.pow, or hard-code).
  async function prepare(api, rec, slip, bits) {
    var out = {};
    var parts = await Promise.all([csrf(api), solve(rec, slip, bits == null ? 0 : bits)]);
    if (parts[0]) out.csrf = parts[0];
    if (parts[1]) out.pow = parts[1];
    return out;
  }
  // Reads the relay's advertised difficulty so the page never needs a hard-coded value.
  async function difficulty(api) {
    try {
      var r = await fetch(api.replace(/\/$/, '') + '/health', { cache: 'no-store', credentials: 'omit' });
      var j = await r.json(); var p = j && j.sec && j.sec.pow;
      var n = typeof p === 'string' ? parseInt(p, 10) : 0; return Number.isFinite(n) ? n : 0;
    } catch (e) { return 0; }
  }
  root.DonateSecurity = { canon: canon, solve: solve, csrf: csrf, prepare: prepare, difficulty: difficulty };
})(typeof window !== 'undefined' ? window : globalThis);
