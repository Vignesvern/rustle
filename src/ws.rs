//! WebSocket handling: one async task pair per connected client, now room-aware.
//!
//! ## The model (with Go analogies)
//!
//! * A room's `broadcast::Sender` ≈ a Go channel that fans out to *every* subscriber.
//! * `tokio::spawn` ≈ `go func()` — it launches a lightweight async task.
//! * `tokio::select!` ≈ Go's `select {}` — it acts on whichever future finishes first.
//!
//! Each client runs two tasks:
//! 1. write task: the room's broadcast channel is drained into this client's socket.
//! 2. read task: this client's socket is drained into the room's broadcast channel.
//!
//! When either ends (the tab closed), `select!` aborts the other, we leave the room, and
//! we broadcast a "left" notice plus a refreshed roster to whoever remains.

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast::error::RecvError;

use crate::AppState;
use crate::message::{ClientMessage, ServerMessage};

/// Axum handler for `GET /ws`: performs the HTTP→WebSocket upgrade, then hands the live
/// socket to `handle_socket` to run for the connection's lifetime.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();

    // Protocol rule: the first valid frame must be a `join` giving us a room + name.
    // We loop until we get a usable one, or the client disconnects.
    let (room, name) = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(ClientMessage::Join { room, name }) = serde_json::from_str(text.as_str())
                {
                    let room = room.trim();
                    let name = name.trim();
                    if !name.is_empty() {
                        // Default an empty room name to "general".
                        let room = if room.is_empty() { "general" } else { room };
                        break (room.to_owned(), name.to_owned());
                    }
                }
            }
            Some(Ok(_)) => {} // ignore ping/pong/binary before join
            _ => return,      // closed before joining: nothing to clean up
        }
    };

    let conn_id = state.hub.next_conn_id();

    // Register in the room and subscribe. `join` returns our receiver + the new roster.
    let (mut rx, roster) = state.hub.join(&room, conn_id, name.clone());
    tracing::info!(%name, %room, "client joined");

    // Announce the join and push the refreshed roster to everyone in the room.
    state.hub.broadcast(
        &room,
        ServerMessage::system(&room, format!("{name} joined")),
    );
    state
        .hub
        .broadcast(&room, ServerMessage::presence(&room, roster));

    // --- Write task: room broadcast -> this browser -----------------------------------
    let mut write_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    let Ok(json) = serde_json::to_string(&msg) else {
                        continue;
                    };
                    // If the send fails, the browser is gone — end the task.
                    if sink.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                // Slow client fell behind the buffer: skip the dropped messages rather
                // than dropping the connection.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            }
        }
    });

    // --- Read task: this browser -> room broadcast ------------------------------------
    let hub = state.hub.clone();
    let read_room = room.clone();
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

    // Leave the room; if anyone remains, tell them and refresh the roster.
    if let Some(roster) = state.hub.leave(&room, conn_id) {
        state
            .hub
            .broadcast(&room, ServerMessage::system(&room, format!("{name} left")));
        state
            .hub
            .broadcast(&room, ServerMessage::presence(&room, roster));
    }
}
