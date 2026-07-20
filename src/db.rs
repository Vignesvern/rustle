//! Postgres persistence (optional).
//!
//! Uses sqlx's *runtime* query API (not the compile-time `query!` macro), so the crate
//! builds without a live database. Persistence is only active when `DATABASE_URL` is set;
//! otherwise the server runs fully in-memory.

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

/// Fetch up to `limit` most-recent messages for a room, in chronological order,
/// ready to replay to a joining client.
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

    // Rows came back newest-first; reverse to oldest-first for replay.
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
