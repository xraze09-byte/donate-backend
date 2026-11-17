# RZ CLAN — Realtime Relay Server

Small Node.js + WebSocket relay so donation requests from any phone reach
your admin queue live, and your overlay gets alerts, across devices. No
database, no framework — one file (`server.js`).

## ⚠️ Read this before deploying to Vercel

**Vercel's serverless functions cannot hold a real WebSocket connection open.**
Each request runs a function that spins up and shuts down — there is no
long-lived process to keep sockets alive, so connections get dropped
constantly (and Vercel's Node/Edge functions don't support the `ws` library
at all for inbound WebSocket upgrades). This isn't a config issue — it's how
their serverless platform works, so "deploy the WS server to Vercel" isn't
actually possible for a persistent-connection server like this.

**What actually works:**
- Deploy `server.js` to a host that runs a normal, always-on Node process:
  **Render**, **Railway**, or **Fly.io** all have free/cheap tiers and take
  ~5 minutes. Instructions below use Render.
- Keep your `rz-clan-v4-backend.html` file hosted wherever you like —
  **Vercel is perfectly fine for that part**, since it's just a static file.

So: static HTML on Vercel + this relay on Render/Railway/Fly. That's the
real, working setup.

## Deploy the relay (Render, free tier)

1. Push this `server/` folder to a GitHub repo (or a repo containing just
   these two files: `server.js`, `package.json`).
2. On [render.com](https://render.com) → **New → Web Service** → connect
   the repo.
3. Settings:
   - Runtime: Node
   - Build command: `npm install`
   - Start command: `npm start`
   - Instance type: Free is fine to start
4. (Optional but recommended) Add an environment variable `RELAY_TOKEN` set
   to a random string — a second secret on top of the channel key.
5. Deploy. You'll get a URL like `https://rz-clan-relay.onrender.com`.
   Your WebSocket URL is that, with `wss://` and `/ws`:
   `wss://rz-clan-relay.onrender.com/ws`

Railway and Fly.io work the same way — any host that runs `node server.js`
as a persistent process works.

Note: Render's free tier sleeps after ~15 min of no traffic and wakes on
the next request (a few seconds of cold-start delay for the first
connection). Railway/Fly free tiers behave similarly. For a stream that's
live for hours at a time this is unnoticeable after the first connect; if
you want zero cold-start, use a paid instance on any of these.

## Point the HTML app at it

`rz-clan-v4-backend.html` already has a default WebSocket URL baked in as a
placeholder:
```
ws:'wss://YOUR-RELAY-SERVER.example.com/ws'
```
Open the file, find that line near the top of the `<script>` block
(`var DEF={...ws:'wss://YOUR-RELAY-SERVER...`), and replace it with your real
`wss://` URL from Render. Every page (donor `#`, admin `#admin`, overlay
`#overlay?k=...`) will then connect to it automatically — no per-device
settings needed. (Admins can still override the URL later via the Settings
tab in `#admin` if you ever need to switch servers.)

## Test it locally first

```bash
cd server
npm install
npm start
# in the HTML file, temporarily set ws:'ws://localhost:8080/ws'
# open the file directly (or serve it) from two different browsers/devices
# on the same network and try the donor → admin flow
```

## Opt-in hardening (CSRF, proof-of-work, signed admin messages, strict donations)

Everything below is **off by default**: deploying changes nothing until you set the env var. Each feature
also has a `log` mode that counts what *would* be rejected (see `GET /health` -> `sec.hits`) without
dropping anything, so you can watch real traffic before enforcing.

| Env var | Values | What it does |
|---|---|---|
| `CSRF_MODE` | `0` / `log` / `1` | donations must carry a short-lived token from `GET /csrf` (bound to the donor's IP) |
| `POW_BITS` | `0`-`24` (try `14`) | donations must carry a proof-of-work bound to the donation content. Each +1 doubles the work |
| `POW_MODE` | `log` / `1` | `log` counts bad proofs but lets them through (default `1` once `POW_BITS` > 0) |
| `STRICT_DONATION` | `0` / `log` / `1` | rejects links, control/bidi characters, stale timestamps, bad ids and odd decimals in donations |
| `REQUIRE_SIGNED` | `0` / `log` / `1` | `alert` / `cfg` / `resolved` must be shaped like a signed message (`sig` + `nonce` + `ts`) |
| `CSRF_SECRET` | long random string | HMAC key for CSRF tokens. Unset = random per boot, so tokens die on every restart |

The relay holds **no signing key and never verifies a signature**: the overlay and donor page do that.
`REQUIRE_SIGNED` only stops messages that are not even shaped like a signed message.

**The donate page must send the proof before you turn CSRF/PoW on**, or real donations will be dropped.
`client/donate-security.js` does this: `const sec = await DonateSecurity.prepare(API, rec, slip, await DonateSecurity.difficulty(API))`
then send `{t:'donation', d:{rec, slip, ...sec}}`. It returns `{}` if the relay is unreachable, so the form never breaks.

Safe rollout: (1) deploy with nothing set, (2) update the donate page to send `sec`, (3) set the features to `log`,
(4) check `/health` -> `sec.hits` stays near zero for real donors, (5) switch to `1`.

## How it works (so you can trust/debug it)

- The HTML app already builds every donation/approval/alert message with a
  `emit(type, data)` call. That function already sends over a WebSocket if
  one is connected — this server is just the other end of that connection.
- Clients connect to `/ws?ch=<channel-key>`. The channel key is the app's
  existing "Overlay Secret Key" (`CFG.key`) — everyone sharing that key
  (donor page, admin page, overlay) ends up in the same room and only sees
  each other's messages. Different keys never cross.
- On donor submit, the browser now also emits a `donation` message (in
  addition to keeping its own local copy) — the server relays it instantly
  to every other connected browser in the same channel, which is what makes
  it show up in the admin queue in real time.
- The server keeps a short **in-memory backlog** of not-yet-resolved
  donations per channel (max 24h). If the admin's browser reconnects
  (dropped wifi, phone locked, page reload), it replays anything still
  pending so nothing gets lost. Approve/reject/delete on the admin side
  clears that item from the backlog immediately.
- There is **no database** — a server restart or redeploy wipes the
  backlog. For a livestream session that's usually fine (the run is a few
  hours), but if you want donations to survive a server restart, swap the
  in-memory `Map` for Redis (e.g. Upstash, works fine on Render/Railway) —
  ask me if you want that added.
- Optional `RELAY_TOKEN` env var: if set, clients must connect with
  `?tok=<value>` matching it, as a second layer of protection beyond the
  channel key. Leave it unset for the simplest setup.

## Files
- `server.js` — the relay server (see above)
- `package.json` — one dependency: `ws`


## Anti-abuse limits for donors (server-side)

The browser cooldown can be bypassed by talking to the socket directly, so the relay enforces its own limits
on anonymous donor connections (no token). They are keyed on the proxy-appended client IP (rightmost
`X-Forwarded-For`), so reconnecting does not reset them. Admin and listener tokens are never throttled.
Set any value to `0` to disable that limit.

| Env var | Default | Meaning |
|---|---|---|
| `DONATE_PER_IP_MAX` / `DONATE_PER_IP_WINDOW_S` | 6 / 600 | max donations per IP per window |
| `DONATE_MIN_GAP_MS` | 3000 | min gap between two donations from one IP |
| `DONATE_PENDING_CAP` | 200 | max un-approved donations held per room |
| `PUBLIC_CONN_PER_IP` | 8 | max simultaneous donor sockets per IP (extra closed with code 4009) |

Tests: `npm test` (relay + rate-limit suites, real `server.js`).
