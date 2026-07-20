//! Plain HTTP endpoints: health, a read-only lobby API, and auth (register/login).

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth;
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

#[derive(Deserialize)]
pub struct AuthRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    token: String,
    username: String,
}

/// Register a new account, returning a JWT (auto-login).
pub async fn register(
    State(state): State<AppState>,
    Json(req): Json<AuthRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    let pool = state.db.as_ref().ok_or(AppError::AuthUnavailable)?;

    let username = req.username.trim();
    if username.is_empty() || username.chars().count() > state.config.max_name_len {
        return Err(AppError::BadRequest(format!(
            "username must be 1–{} characters",
            state.config.max_name_len
        )));
    }
    if req.password.len() < 6 {
        return Err(AppError::BadRequest(
            "password must be at least 6 characters".to_owned(),
        ));
    }

    let hash = auth::hash_password(&req.password).map_err(|e| {
        tracing::error!(error = %e, "password hashing failed");
        AppError::Internal
    })?;

    match crate::db::create_user(pool, username, &hash).await {
        Ok(_) => {}
        Err(e)
            if e.as_database_error()
                .is_some_and(|d| d.is_unique_violation()) =>
        {
            return Err(AppError::UsernameTaken);
        }
        Err(e) => {
            tracing::error!(error = %e, "create_user failed");
            return Err(AppError::Internal);
        }
    }

    let token = issue(&state, username)?;
    Ok(Json(AuthResponse {
        token,
        username: username.to_owned(),
    }))
}

/// Log in with an existing account, returning a JWT.
pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<AuthRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    let pool = state.db.as_ref().ok_or(AppError::AuthUnavailable)?;

    let user = crate::db::find_user(pool, req.username.trim())
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "find_user failed");
            AppError::Internal
        })?
        .ok_or(AppError::InvalidCredentials)?;

    if !auth::verify_password(&req.password, &user.password_hash) {
        return Err(AppError::InvalidCredentials);
    }

    let token = issue(&state, &user.username)?;
    Ok(Json(AuthResponse {
        token,
        username: user.username,
    }))
}

/// Issue a JWT, mapping signing failures to a 500.
fn issue(state: &AppState, username: &str) -> Result<String, AppError> {
    auth::issue_token(
        &state.config.jwt_secret,
        username,
        state.config.jwt_ttl_secs,
    )
    .map_err(|e| {
        tracing::error!(error = %e, "token issuance failed");
        AppError::Internal
    })
}
