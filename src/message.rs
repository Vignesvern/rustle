//! The wire protocol: the JSON messages the browser and server exchange.
//!
//! Each direction is an enum; serde's `#[serde(tag = "type")]` tags every variant with a
//! `type` field on the wire (e.g. `{"type":"join",...}`), like a discriminated union.

use serde::{Deserialize, Serialize};

/// Messages the client (browser) sends to the server.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// First message a client must send: which room to join and their display name.
    Join { room: String, name: String },
    /// A chat message the user typed (delivered to the room they joined).
    Message { body: String },
}

/// Messages the server sends to clients.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// A chat message authored by `name` in `room`.
    Message {
        room: String,
        name: String,
        body: String,
        /// RFC 3339 timestamp, stamped server-side so every client agrees on ordering.
        ts: String,
    },
    /// A system notice, e.g. "alice joined".
    System { room: String, body: String },
    /// The current members of `room`, re-sent whenever the roster changes.
    Presence { room: String, users: Vec<String> },
}

impl ServerMessage {
    /// A chat message with a fresh server timestamp.
    pub fn chat(room: impl Into<String>, name: impl Into<String>, body: impl Into<String>) -> Self {
        ServerMessage::Message {
            room: room.into(),
            name: name.into(),
            body: body.into(),
            ts: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// A system notice scoped to a room.
    pub fn system(room: impl Into<String>, body: impl Into<String>) -> Self {
        ServerMessage::System {
            room: room.into(),
            body: body.into(),
        }
    }

    /// A presence (roster) update for a room.
    pub fn presence(room: impl Into<String>, users: Vec<String>) -> Self {
        ServerMessage::Presence {
            room: room.into(),
            users,
        }
    }
}
