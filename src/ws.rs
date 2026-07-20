//! WebSocket handling: one async task pair per connected client.
//!
//! ## The model (with Go analogies)
//!
//! * A room's `broadcast::Sender` ≈ a Go channel that fans out to *every* subscriber.
//! * `tokio::spawn` ≈ `go func()`; `tokio::select!` ≈ Go's `select {}`.
//!
//! ## Auth
//!
//! A `?token=<jwt>` query parameter is optional. If absent, the client is a guest and its
//! display name comes from the `join` frame. If present, it must be valid (else the
//! upgrade is rejected with 401), and the authenticated username — not any client-supplied
//! name — is used, so identities can't be spoofed.

use std::time::{Duration, Instant};

use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::AppState;
use crate::config::Config;
use crate::error::AppError;
use crate::message::{ClientMessage, ServerMessage};

#[derive(Deserialize)]
pub struct WsQuery {
    token: Option<String>,
}

/// Axum handler for `GET /ws`: authenticates (if a token is supplied), then upgrades.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<AppState>,
) -> Response {
    // Resolve the authenticated user, if any. A supplied-but-invalid token is rejected.
    let auth_user = match query.token {
        Some(token) => match crate::auth::verify_token(&state.config.jwt_secret, &token) {
            Some(user) => Some(user),
            None => return AppError::Unauthorized.into_response(),
        },
        None => None,
    };
    ws.on_upgrade(move |socket| handle_socket(socket, state, auth_user))
}

async fn handle_socket(socket: WebSocket, state: AppState, auth_user: Option<String>) {
    let (mut sink, mut stream) = socket.split();
    let config = state.config.clone();

    // The first valid frame must be a `join`. For authenticated clients the trusted
    // username overrides whatever name the client sends.
    let (room, name) = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(ClientMessage::Join { room, name }) = serde_json::from_str(text.as_str())
                {
                    let effective_name = auth_user.clone().unwrap_or(name);
                    match validate_join(&room, &effective_name, &config) {
                        Ok(pair) => break pair,
                        Err(reason) => {
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
    tracing::info!(%name, %room, authenticated = auth_user.is_some(), "client joined");

    // Replay recent history to *this* client before live traffic starts.
    if let Some(pool) = &state.db {
        match crate::db::recent_history(pool, &room, config.history_limit).await {
            Ok(history) => {
                for msg in history {
                    if let Ok(json) = serde_json::to_string(&msg)
                        && sink.send(Message::Text(json.into())).await.is_err()
                    {
                        state.hub.leave(&room, conn_id);
                        return;
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to load history"),
        }
    }

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
                Some(note) = notify_rx.recv() => note,
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

    // --- Read task: this browser -> room broadcast (size cap + rate limit + persist) --
    let hub = state.hub.clone();
    let db = state.db.clone();
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

                if let Some(pool) = &db
                    && let Err(e) = crate::db::insert_message(pool, &read_room, &author, body).await
                {
                    tracing::warn!(error = %e, "failed to persist message");
                }
            }
        }
    });

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
