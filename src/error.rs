//! Application error type and its HTTP representation.
//!
//! `thiserror` derives the `Display`/`Error` boilerplate; `IntoResponse` maps each
//! variant to an HTTP status + JSON body, so handlers can just `return Err(...)`.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("room '{0}' not found")]
    RoomNotFound(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("username is already taken")]
    UsernameTaken,
    #[error("invalid username or password")]
    InvalidCredentials,
    #[error("authentication required")]
    Unauthorized,
    #[error("authentication requires a configured database")]
    AuthUnavailable,
    #[error("internal server error")]
    Internal,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::RoomNotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::UsernameTaken => StatusCode::CONFLICT,
            AppError::InvalidCredentials | AppError::Unauthorized => StatusCode::UNAUTHORIZED,
            AppError::AuthUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
