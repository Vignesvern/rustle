//! The wire protocol: the JSON messages the browser and server exchange.
//!
//! We model each direction as an `enum`. serde's `#[serde(tag = "type")]` turns a
//! variant like `Join { name }` into `{"type":"join","name":"..."}` on the wire.
//! This is the Rust equivalent of a tagged union / discriminated union — the `type`
//! field tells the receiver which variant it's looking at.

use serde::{Deserialize, Serialize};

/// Messages the **client** (browser) sends *to* the server.
///
/// `Deserialize` = "can be parsed from JSON". We only need to read these, not write them.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// First message a client must send: announces their display name.
    Join { name: String },
    /// A chat message the user typed.
    Message { body: String },
}

/// Messages the **server** broadcasts *to* clients.
///
/// `Clone` is required because a `broadcast` channel hands out a copy to every
/// subscriber. `Serialize` = "can be turned into JSON" so we can send it over the socket.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// A chat message authored by `name`.
    Message {
        name: String,
        body: String,
        /// RFC 3339 timestamp, stamped server-side so every client agrees on ordering.
        ts: String,
    },
    /// A system notice, e.g. "alice joined".
    System { body: String },
}

impl ServerMessage {
    /// Convenience constructor for a chat message with a fresh server timestamp.
    pub fn chat(name: impl Into<String>, body: impl Into<String>) -> Self {
        ServerMessage::Message {
            name: name.into(),
            body: body.into(),
            ts: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Convenience constructor for a system notice.
    pub fn system(body: impl Into<String>) -> Self {
        ServerMessage::System { body: body.into() }
    }
}
