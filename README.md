# rustle

A real-time chat service written in **Rust** — WebSocket-based, with per-room broadcast
fan-out, live presence, configurable limits, and an integration test suite. Built with
[`axum`](https://github.com/tokio-rs/axum) and [`tokio`](https://tokio.rs).

> Portfolio project. Built incrementally in milestones; see the roadmap below.

## What it does

Open the app in two browser tabs, pick a name and a room, and chat in real time.
Messages are fanned out to everyone **in the same room**; join/leave events show up as
system notices, and a live "online" roster tracks who's present. Messages are size-capped
and rate-limited per connection.

## Run it

```bash
cargo run
# then open http://localhost:3000 in two browser tabs
```

Set `RUST_LOG=rustle=debug` for verbose logs.

## How it works

Each connected browser opens a WebSocket to `/ws` and joins one room. On the server, a
[`Hub`](src/hub.rs) registry maps each room name to its own
[`tokio::sync::broadcast`](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
channel and member roster — a `HashMap` guarded by an `RwLock`, shared across connections
via `Arc`. Each connection runs **two async tasks**:

- a **read task** — parses incoming frames and publishes them to its room's channel
  (enforcing the size cap and rate limit)
- a **write task** — forwards that room's broadcast messages, plus any private notices,
  out to the client's socket

`tokio::select!` joins the two tasks so that when one ends (the tab closes), the other is
aborted, the member is removed (empty rooms are pruned), and a refreshed roster is
broadcast to whoever remains.

```
browser ──ws──▶ read task ──▶ room broadcast::Sender ──▶ write task ──ws──▶ same-room browsers
```

## HTTP API

| Method | Path               | Description                         |
|--------|--------------------|-------------------------------------|
| GET    | `/health`          | Liveness probe (returns `ok`)       |
| GET    | `/api/rooms`       | List active rooms + member counts   |
| GET    | `/api/rooms/{name}`| A room's roster, or `404` if unknown|
| GET    | `/ws`              | WebSocket endpoint                  |

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
| `RUSTLE_ADDR`                   | `0.0.0.0:3000` | Bind address                    |
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

The suite spins up the server on an ephemeral port and drives it with real
`tokio-tungstenite` clients (broadcast, room isolation, presence, rate limiting, size
limits) and exercises the HTTP endpoints via axum's `oneshot`.

## Tech stack

`tokio` · `axum` (WebSockets) · `serde` / `serde_json` · `tracing` · `tower-http` ·
`thiserror` · `tokio-tungstenite` (tests)

## Roadmap

- [x] **M1** — single-room broadcast chat + web client
- [x] **M2** — multiple rooms + presence
- [x] **M3** — config, rate limiting, size limits, HTTP API, integration tests
- [ ] **M4** — Postgres persistence + message history
- [ ] **M5** — accounts + JWT auth
- [ ] **M6** — Docker, CI, deploy
