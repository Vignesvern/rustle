//! rustle — a real-time WebSocket chat server.
//!
//! The building blocks live in the library so that both the binary (`main.rs`) and the
//! integration tests can construct and drive the same app via [`build_app`].

pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod hub;
pub mod message;
pub mod routes;
pub mod ws;

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use sqlx::PgPool;
use tower_http::{services::ServeDir, trace::TraceLayer};

use crate::config::Config;
use crate::hub::Hub;

/// Shared application state, cloned once per request by axum. Every field is a cheap
/// shared handle (`Arc`, or an `Option<PgPool>` which is itself `Arc`-backed).
#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<Hub>,
    pub config: Arc<Config>,
    /// Optional Postgres pool; `None` means the server runs without persistence.
    pub db: Option<PgPool>,
}

impl AppState {
    /// Build state from a config, sizing the hub's broadcast buffers to match. No database.
    pub fn new(config: Config) -> Self {
        Self {
            hub: Arc::new(Hub::new(config.room_capacity)),
            config: Arc::new(config),
            db: None,
        }
    }

    /// Attach (or clear) the database pool.
    pub fn with_db(mut self, db: Option<PgPool>) -> Self {
        self.db = db;
        self
    }
}

/// Assemble the router: HTTP endpoints, the WebSocket route, and static files.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(routes::health))
        .route("/api/register", post(routes::register))
        .route("/api/login", post(routes::login))
        .route("/api/rooms", get(routes::list_rooms))
        .route("/api/rooms/{name}", get(routes::room_detail))
        .route("/ws", get(ws::ws_handler))
        .fallback_service(ServeDir::new("static"))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
