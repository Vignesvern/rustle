//! Plain HTTP endpoints (everything that isn't the WebSocket): a health check and a
//! small read-only "lobby" API over the live room registry.

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use serde::Serialize;

use crate::AppState;
use crate::error::AppError;

/// Liveness probe. Handy for Docker/K8s and uptime checks.
pub async fn health() -> impl IntoResponse {
    "ok"
}

/// List all active rooms and their member counts.
pub async fn list_rooms(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.hub.rooms_summary())
}

#[derive(Serialize)]
pub struct RoomDetail {
    room: String,
    users: Vec<String>,
}

/// Return the roster of a single room, or 404 (via [`AppError`]) if it doesn't exist.
pub async fn room_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<RoomDetail>, AppError> {
    match state.hub.room_roster(&name) {
        Some(users) => Ok(Json(RoomDetail { room: name, users })),
        None => Err(AppError::RoomNotFound(name)),
    }
}
