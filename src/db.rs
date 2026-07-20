//! Postgres persistence (optional).
//!
//! Uses sqlx's *runtime* query API (not the compile-time `query!` macro), so the crate
//! builds without a live database.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::message::ServerMessage;

/// Connect to Postgres and run pending migrations.
pub async fn init(url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

/// A persisted message row.
#[derive(sqlx::FromRow)]
struct MessageRow {
    room: String,
    author: String,
    body: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// A user account row.
#[derive(sqlx::FromRow)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password_hash: String,
}

/// Persist a single chat message.
pub async fn insert_message(
    pool: &PgPool,
    room: &str,
    author: &str,
    body: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO messages (room, author, body) VALUES ($1, $2, $3)")
        .bind(room)
        .bind(author)
        .bind(body)
        .execute(pool)
        .await?;
    Ok(())
}

/// Fetch up to `limit` most-recent messages for a room, in chronological order.
pub async fn recent_history(
    pool: &PgPool,
    room: &str,
    limit: usize,
) -> Result<Vec<ServerMessage>, sqlx::Error> {
    let rows = sqlx::query_as::<_, MessageRow>(
        "SELECT room, author, body, created_at FROM messages \
         WHERE room = $1 ORDER BY id DESC LIMIT $2",
    )
    .bind(room)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    let msgs = rows
        .into_iter()
        .rev()
        .map(|r| ServerMessage::Message {
            room: r.room,
            name: r.author,
            body: r.body,
            ts: r.created_at.to_rfc3339(),
        })
        .collect();
    Ok(msgs)
}

/// Create a user. The caller distinguishes a unique-violation (username taken) via
/// [`sqlx::error::DatabaseError::is_unique_violation`].
pub async fn create_user(
    pool: &PgPool,
    username: &str,
    password_hash: &str,
) -> Result<i64, sqlx::Error> {
    let row: (i64,) =
        sqlx::query_as("INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id")
            .bind(username)
            .bind(password_hash)
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

/// Look up a user by username.
pub async fn find_user(pool: &PgPool, username: &str) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>("SELECT id, username, password_hash FROM users WHERE username = $1")
        .bind(username)
        .fetch_optional(pool)
        .await
}
