//! rustle — a real-time WebSocket chat server.
//!
//! M1: a single global room. Every connected browser subscribes to one broadcast
//! channel; anything one client says is fanned out to all the others.

mod message;
mod ws;

use axum::{Router, routing::get};
use tokio::sync::broadcast;
use tower_http::{services::ServeDir, trace::TraceLayer};

use crate::message::ServerMessage;

/// Shared application state. A `broadcast::Sender` is just an `Arc` inside, so cloning
/// `AppState` is cheap — which matters because axum hands each request its own clone.
#[derive(Clone)]
pub struct AppState {
    /// The single global room's broadcast channel (M1). One sender, many receivers.
    pub tx: broadcast::Sender<ServerMessage>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Structured logging. Tune verbosity with RUST_LOG, e.g. `RUST_LOG=rustle=debug`.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustle=info,tower_http=info".into()),
        )
        .init();

    // The broadcast channel. Capacity (100) is how many messages a slow subscriber may
    // fall behind before it starts dropping the oldest ones.
    let (tx, _rx) = broadcast::channel::<ServerMessage>(100);
    let state = AppState { tx };

    // /ws is the WebSocket endpoint; every other path is served from ./static,
    // so GET / returns static/index.html (the chat UI).
    let app = Router::new()
        .route("/ws", get(ws::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("rustle listening on http://localhost:3000");

    axum::serve(listener, app).await?;
    Ok(())
}
