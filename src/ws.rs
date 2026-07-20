//! WebSocket handling: one async task pair per connected client.
//!
//! ## The model (with Go analogies)
//!
//! * A room's `broadcast::Sender` ≈ a Go channel that fans out to *every* subscriber.
//! * `tokio::spawn` ≈ `go func()`; `tokio::select!` ≈ Go's `select {}`.
//!
//! Each client runs two tasks:
//! 1. write task: the room broadcast *and* a private notice channel are drained into the
//!    client's socket (the private channel carries per-client warnings).
//! 2. read task: the client's socket is drained into the room broadcast, subject to a
//!    message-size cap and a per-connection rate limit.

use std::time::{Duration, Instant};

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::AppState;
use crate::config::Config;
use crate::message::{ClientMessage, ServerMessage};

/// Axum handler for `GET /ws`: upgrades the connection, then runs `handle_socket`.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();
    let config = state.config.clone();

    // The first valid frame must be a `join` with an acceptable room + name.
    let (room, name) = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(ClientMessage::Join { room, name }) = serde_json::from_str(text.as_str())
                {
                    match validate_join(&room, &name, &config) {
                        Ok(pair) => break pair,
                        Err(reason) => {
                            // Tell the client why we're rejecting, then close.
                            let msg = ServerMessage::system("", reason);
                            if let Ok(json) = serde_json::to_string(&msg) {
                                let _ = sink.send(Message::Text(json.into())).await;
                            }
                            return;
                        }
                    }
                }
            }
            Some(Ok(_)) => {} // ignore ping/pong/binary before join
            _ => return,      // closed before joining
        }
    };

    let conn_id = state.hub.next_conn_id();
    let (mut rx, roster) = state.hub.join(&room, conn_id, name.clone());
    tracing::info!(%name, %room, "client joined");

    state.hub.broadcast(
        &room,
        ServerMessage::system(&room, format!("{name} joined")),
    );
    state
        .hub
        .broadcast(&room, ServerMessage::presence(&room, roster));

    // Private server -> this-client channel for warnings (rate-limit / oversize notices).
    let (notify_tx, mut notify_rx) = mpsc::channel::<ServerMessage>(8);

    // --- Write task: room broadcast + private notices -> this browser -----------------
    let mut write_task = tokio::spawn(async move {
        loop {
            let msg = tokio::select! {
                // A private notice destined only for this client.
                Some(note) = notify_rx.recv() => note,
                // A room broadcast. Skip lagged messages; stop when the channel closes.
                r = rx.recv() => match r {
                    Ok(msg) => msg,
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                },
            };
            let Ok(json) = serde_json::to_string(&msg) else {
                continue;
            };
            if sink.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // --- Read task: this browser -> room broadcast (size cap + rate limit) ------------
    let hub = state.hub.clone();
    let read_room = room.clone();
    let author = name.clone();
    let max_bytes = config.max_message_bytes;
    let mut limiter = RateLimiter::new(config.rate_limit_max, config.rate_limit_window);
    let mut read_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg
                && let Ok(ClientMessage::Message { body }) =
                    serde_json::from_str::<ClientMessage>(text.as_str())
            {
                let body = body.trim();
                if body.is_empty() {
                    continue;
                }
                if body.len() > max_bytes {
                    let note = ServerMessage::system(
                        &read_room,
                        format!("message too long (max {max_bytes} bytes)"),
                    );
                    let _ = notify_tx.send(note).await;
                    continue;
                }
                if !limiter.allow() {
                    let note =
                        ServerMessage::system(&read_room, "you're sending messages too fast");
                    let _ = notify_tx.send(note).await;
                    continue;
                }
                hub.broadcast(
                    &read_room,
                    ServerMessage::chat(&read_room, author.clone(), body),
                );
            }
        }
    });

    // Whichever task finishes first, abort the other — graceful teardown.
    tokio::select! {
        _ = &mut write_task => read_task.abort(),
        _ = &mut read_task => write_task.abort(),
    }

    tracing::info!(%name, %room, "client left");
    if let Some(roster) = state.hub.leave(&room, conn_id) {
        state
            .hub
            .broadcast(&room, ServerMessage::system(&room, format!("{name} left")));
        state
            .hub
            .broadcast(&room, ServerMessage::presence(&room, roster));
    }
}

/// Validate a join against configured limits. Returns `(room, name)` on success (with an
/// empty room defaulted to "general"), or a human-readable reason on failure.
fn validate_join(room: &str, name: &str, config: &Config) -> Result<(String, String), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("name cannot be empty".to_owned());
    }
    if name.chars().count() > config.max_name_len {
        return Err(format!("name too long (max {} chars)", config.max_name_len));
    }

    let room = room.trim();
    let room = if room.is_empty() { "general" } else { room };
    if room.chars().count() > config.max_room_len {
        return Err(format!(
            "room name too long (max {} chars)",
            config.max_room_len
        ));
    }

    Ok((room.to_owned(), name.to_owned()))
}

/// A per-connection fixed-window rate limiter.
struct RateLimiter {
    max: u32,
    window: Duration,
    count: u32,
    window_start: Instant,
}

impl RateLimiter {
    fn new(max: u32, window: Duration) -> Self {
        Self {
            max,
            window,
            count: 0,
            window_start: Instant::now(),
        }
    }

    /// Returns `true` if a message is allowed now, consuming one unit of quota.
    fn allow(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.count = 0;
        }
        if self.count < self.max {
            self.count += 1;
            true
        } else {
            false
        }
    }
}
