//! rustle — a real-time WebSocket chat server.
//!
//! The building blocks live in the library so that both the binary (`main.rs`) and the
//! integration tests can construct and drive the same app via [`build_app`].

pub mod config;
pub mod error;
pub mod hub;
pub mod message;
pub mod routes;
pub mod ws;

use std::sync::Arc;

use axum::{Router, routing::get};
use tower_http::{services::ServeDir, trace::TraceLayer};

use crate::config::Config;
use crate::hub::Hub;

/// Shared application state. Both fields are cheap-to-clone shared handles; axum hands
/// each request its own clone of `AppState`.
#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<Hub>,
    pub config: Arc<Config>,
}

impl AppState {
    /// Build state from a config, sizing the hub's broadcast buffers to match.
    pub fn new(config: Config) -> Self {
        Self {
            hub: Arc::new(Hub::new(config.room_capacity)),
            config: Arc::new(config),
        }
    }
}

/// Assemble the router: HTTP endpoints, the WebSocket route, and static files.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(routes::health))
        .route("/api/rooms", get(routes::list_rooms))
        .route("/api/rooms/{name}", get(routes::room_detail))
        .route("/ws", get(ws::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
