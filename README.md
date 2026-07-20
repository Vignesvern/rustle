# rustle

A real-time chat service written in **Rust** — WebSocket-based, with per-room broadcast
fan-out and live presence. Built with [`axum`](https://github.com/tokio-rs/axum)
and [`tokio`](https://tokio.rs).

> Portfolio project. Built incrementally in milestones; see the roadmap below.

## What it does

Open the app in two browser tabs, pick a name and a room, and chat in real time.
Messages are fanned out to everyone **in the same room**; join/leave events show up as
system notices, and a live "online" roster tracks who's present.

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
- a **write task** — forwards that room's broadcast messages out to the client's socket

`tokio::select!` joins the two tasks so that when one ends (the tab closes), the other is
aborted, the member is removed from the room (empty rooms are pruned), and a refreshed
roster is broadcast to whoever remains.

```
browser ──ws──▶ read task ──▶ room broadcast::Sender ──▶ write task ──ws──▶ same-room browsers
```

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

## Tech stack

`tokio` · `axum` (WebSockets) · `serde` / `serde_json` · `tracing` · `tower-http`

## Roadmap

- [x] **M1** — single-room broadcast chat + web client
- [x] **M2** — multiple rooms + presence
- [ ] **M3** — robustness, config, rate limiting, integration tests
- [ ] **M4** — Postgres persistence + message history
- [ ] **M5** — accounts + JWT auth
- [ ] **M6** — Docker, CI, deploy
