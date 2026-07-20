# rustle

A real-time chat service written in **Rust** — WebSocket-based, with broadcast
fan-out to every connected client. Built with [`axum`](https://github.com/tokio-rs/axum)
and [`tokio`](https://tokio.rs).

> Portfolio project. Built incrementally in milestones; see the roadmap below.

## What it does

Open the app in two browser tabs, pick a name in each, and chat in real time.
Messages are fanned out to every connected client over WebSockets; join/leave
events show up as system notices.

## Run it

```bash
cargo run
# then open http://localhost:3000 in two browser tabs
```

Set `RUST_LOG=rustle=debug` for verbose logs.

## How it works

Each connected browser opens a WebSocket to `/ws`. On the server, every client
subscribes to a single [`tokio::sync::broadcast`](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
channel. Each connection runs **two async tasks**:

- a **read task** — parses incoming frames and publishes them to the broadcast channel
- a **write task** — forwards broadcast messages out to that client's socket

`tokio::select!` joins the two tasks so that when one ends (the tab closes), the
other is aborted and the connection is torn down cleanly.

```
browser ──ws──▶ read task ──▶ broadcast::Sender ──▶ write task ──ws──▶ every browser
```

## Wire protocol (JSON over WebSocket)

Client → server:

```json
{ "type": "join", "name": "alice" }
{ "type": "message", "body": "hello" }
```

Server → client:

```json
{ "type": "message", "name": "alice", "body": "hello", "ts": "2026-07-20T10:14:56Z" }
{ "type": "system", "body": "alice joined" }
```

## Tech stack

`tokio` · `axum` (WebSockets) · `serde` / `serde_json` · `tracing` · `tower-http`

## Roadmap

- [x] **M1** — single-room broadcast chat + web client
- [ ] **M2** — multiple rooms + presence
- [ ] **M3** — robustness, config, rate limiting, integration tests
- [ ] **M4** — Postgres persistence + message history
- [ ] **M5** — accounts + JWT auth
- [ ] **M6** — Docker, CI, deploy
