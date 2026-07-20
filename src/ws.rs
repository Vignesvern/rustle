//! WebSocket handling: one async task pair per connected client.
//!
//! ## The model (with Go analogies)
//!
//! * `broadcast::Sender` ≈ a Go channel that fans out to *every* subscriber. Send once,
//!   every connected client receives a copy.
//! * `tokio::spawn` ≈ `go func()` — it launches a lightweight async task.
//! * `tokio::select!` ≈ Go's `select {}` — it waits on multiple futures and acts on
//!   whichever finishes first.
//!
//! Each client gets **two** tasks:
//! 1. write task: the broadcast channel is drained into this client's socket (fan-out).
//! 2. read task: this client's socket is drained into the broadcast channel.
//!
//! When either task ends (the browser closed the tab, or the send failed), `select!`
//! wakes up and we abort the other task so nothing leaks.

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};

use crate::AppState;
use crate::message::{ClientMessage, ServerMessage};

/// Axum handler for `GET /ws`. Performs the HTTP→WebSocket upgrade handshake, then
/// hands the live socket to `handle_socket`. `on_upgrade` returns immediately; the
/// closure runs in the background once the upgrade completes.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Drives a single client connection for its entire lifetime.
async fn handle_socket(socket: WebSocket, state: AppState) {
    // Split the socket into a write half (`sink`) and a read half (`stream`) so the two
    // tasks below can own them independently. `.split()` comes from the StreamExt trait.
    let (mut sink, mut stream) = socket.split();

    // Subscribe *before* doing anything else so we don't miss messages sent while we're
    // still setting up. Every subscriber gets its own receiver.
    let mut rx = state.tx.subscribe();

    // Protocol rule: the first valid message must be a `join` that gives us a name.
    // We loop until we get one (ignoring stray frames), or the client disconnects.
    let name = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(ClientMessage::Join { name }) = serde_json::from_str(text.as_str()) {
                    break name;
                }
                // Not a join yet — keep waiting.
            }
            Some(Ok(_)) => {} // ignore ping/pong/binary before join
            _ => return,      // stream closed or errored before joining: nothing to clean up
        }
    };

    tracing::info!(%name, "client joined");

    // Tell everyone (including this client) that someone joined.
    let _ = state
        .tx
        .send(ServerMessage::system(format!("{name} joined")));

    // --- Write task: broadcast channel -> this browser --------------------------------
    let mut write_task = tokio::spawn(async move {
        // `rx.recv()` yields the next broadcast message. It errors only when the channel
        // is closed or this receiver lagged too far behind; either way we stop.
        while let Ok(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(_) => continue,
            };
            // If the send fails, the browser is gone — end the task.
            if sink.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // --- Read task: this browser -> broadcast channel ---------------------------------
    let tx = state.tx.clone();
    let author = name.clone();
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
                // Broadcast to everyone. `send` errors only if there are no receivers,
                // which can't happen here (we hold one), so ignoring the result is fine.
                let _ = tx.send(ServerMessage::chat(author.clone(), body));
            }
        }
    });

    // Wait for whichever task finishes first, then abort the other. This is the graceful
    // teardown: if the browser disconnects, `read_task` ends -> we abort `write_task`.
    tokio::select! {
        _ = &mut write_task => read_task.abort(),
        _ = &mut read_task => write_task.abort(),
    }

    tracing::info!(%name, "client left");
    let _ = state.tx.send(ServerMessage::system(format!("{name} left")));
}
