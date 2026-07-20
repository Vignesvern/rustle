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
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::RoomNotFound(_) => StatusCode::NOT_FOUND,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
