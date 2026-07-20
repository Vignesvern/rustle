# rustle

[![CI](https://github.com/Vignesvern/rustle/actions/workflows/ci.yml/badge.svg)](https://github.com/Vignesvern/rustle/actions/workflows/ci.yml)

A real-time chat service written in **Rust** — WebSocket-based, with per-room broadcast
fan-out, live presence, configurable limits, optional Postgres persistence, JWT-based
accounts, Docker packaging, and CI. Built with
[`axum`](https://github.com/tokio-rs/axum) and [`tokio`](https://tokio.rs).

> Portfolio project. Built incrementally in milestones; see the roadmap below.

## What it does

Open the app, sign in (or continue as a guest), pick a room, and chat in real time.
Messages are fanned out to everyone **in the same room**; join/leave events show up as
system notices, and a live "online" roster tracks who's present. Messages are size-capped
and rate-limited per connection. With a database configured, recent history is replayed to
clients on join, and accounts are persisted.

## Quick start

### With Docker (app + Postgres)

```bash
docker compose up --build
# open http://localhost:3000
```

### From source

```bash
cargo run
# open http://localhost:3000 — sign up, or "Continue as guest"
```

Without a database it runs fully in-memory (guests only; auth endpoints need a database).
To enable persistence + accounts from source:

```bash
docker compose up -d db
export DATABASE_URL=postgres://rustle:rustle@localhost:5432/rustle
export RUSTLE_JWT_SECRET=$(openssl rand -hex 32)
cargo run
```

## Accounts & auth

- `POST /api/register` and `POST /api/login` take `{ "username", "password" }` and return
  `{ "token", "username" }`. Passwords are hashed with **argon2id**; the token is a
  short-lived **HS256 JWT**.
- The WebSocket accepts an optional `?token=<jwt>`. With a valid token, the client's
  identity is taken from the token (it can't be spoofed via the join frame); an invalid
  token is rejected with `401`. With no token, the client is a **guest**.

## How it works

Each connected browser opens a WebSocket to `/ws` and joins one room. On the server, a
[`Hub`](src/hub.rs) registry maps each room name to its own
[`tokio::sync::broadcast`](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
channel and member roster — a `HashMap` guarded by an `RwLock`, shared across connections
via `Arc`. Each connection runs **two async tasks**:

- a **read task** — parses incoming frames and publishes them to its room's channel
  (enforcing the size cap and rate limit, and persisting to Postgres if configured)
- a **write task** — forwards that room's broadcast messages, plus any private notices,
  out to the client's socket

`tokio::select!` joins the two tasks so that when one ends (the tab closes), the other is
aborted, the member is removed (empty rooms are pruned), and a refreshed roster is
broadcast to whoever remains.

## HTTP API

| Method | Path               | Description                              |
|--------|--------------------|------------------------------------------|
| POST   | `/api/register`    | Create an account → `{ token, username }`|
| POST   | `/api/login`       | Log in → `{ token, username }`           |
| GET    | `/health`          | Liveness probe (returns `ok`)            |
| GET    | `/api/rooms`       | List active rooms + member counts        |
| GET    | `/api/rooms/{name}`| A room's roster, or `404` if unknown     |
| GET    | `/ws`              | WebSocket endpoint (optional `?token=`)  |

## Wire protocol (JSON over WebSocket)

Client → server:

```json
{ "type": "join", "room": "general", "name": "alice" }
{ "type": "message", "body": "hello" }
```

Server → client:

```json
{ "type": "message", "room": "general", "name": "alice", "body": "hello", "ts": "2026-07-20T10:14:56Z" }
{ "type": "system", "room": "general", "body": "alice joined" }
{ "type": "presence", "room": "general", "users": ["alice", "bob"] }
```

## Configuration

All settings are environment variables with sensible defaults (see [`config.rs`](src/config.rs)):

| Variable                        | Default | Meaning                                |
|---------------------------------|---------|----------------------------------------|
| `RUSTLE_ADDR`                   | `0.0.0.0:3000` | Bind address (Docker image: `:8080`) |
| `DATABASE_URL`                  | *(unset)* | Postgres URL; unset = no persistence |
| `RUSTLE_JWT_SECRET`             | *(dev default)* | HS256 signing secret — **set in prod** |
| `RUSTLE_JWT_TTL_SECS`           | `86400` | Token lifetime                         |
| `RUSTLE_HISTORY_LIMIT`          | `50`    | Messages replayed to a client on join  |
| `RUSTLE_MAX_MESSAGE_BYTES`      | `4096`  | Max chat message size                  |
| `RUSTLE_MAX_NAME_LEN`           | `24`    | Max display-name length (chars)        |
| `RUSTLE_MAX_ROOM_LEN`           | `24`    | Max room-name length (chars)           |
| `RUSTLE_ROOM_CAPACITY`          | `128`   | Per-room broadcast buffer              |
| `RUSTLE_RATE_LIMIT_MAX`         | `10`    | Messages allowed per window            |
| `RUSTLE_RATE_LIMIT_WINDOW_SECS` | `10`    | Rate-limit window (seconds)            |

## Testing

```bash
cargo test        # integration tests: real WebSocket clients + HTTP endpoints
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs fmt, clippy, and the full
test suite against a Postgres service on every push and PR. Persistence and account tests
run only when `DATABASE_URL` is set (otherwise they skip).

## Deploy (Fly.io)

A [`fly.toml`](fly.toml) template is included. Roughly:

```bash
fly launch --no-deploy                        # create the app
fly postgres create && fly postgres attach <db-app>   # provisions DATABASE_URL
fly secrets set RUSTLE_JWT_SECRET=$(openssl rand -hex 32)
fly deploy
```

The multi-stage [`Dockerfile`](Dockerfile) is used for both `docker compose` and the
Fly deploy; the final image is a slim Debian with just the binary + `static/`.

## Tech stack

`tokio` · `axum` (WebSockets) · `serde` / `serde_json` · `sqlx` (Postgres) · `argon2` ·
`jsonwebtoken` · `tracing` · `tower-http` · `thiserror` · `tokio-tungstenite` (tests) ·
Docker · GitHub Actions

## Roadmap

- [x] **M1** — single-room broadcast chat + web client
- [x] **M2** — multiple rooms + presence
- [x] **M3** — config, rate limiting, size limits, HTTP API, integration tests
- [x] **M4** — Postgres persistence + message history
- [x] **M5** — accounts + JWT auth
- [x] **M6** — Docker, CI, deploy
