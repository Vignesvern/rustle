//! rustle — a real-time WebSocket chat server.
//!
//! M2: multiple named rooms with live presence. Each connection joins one room; the
//! [`Hub`] registry maps room names to per-room broadcast channels and member rosters.

mod hub;
mod message;
mod ws;

use std::sync::Arc;

use axum::{Router, routing::get};
use tower_http::{services::ServeDir, trace::TraceLayer};

use crate::hub::Hub;

/// Shared application state. `Arc<Hub>` is a cheap-to-clone shared pointer to the one
/// room registry; axum hands each request its own clone.
#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<Hub>,
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

    let state = AppState {
        hub: Arc::new(Hub::default()),
    };

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
