//! Runtime configuration, read from environment variables with sensible defaults.
//!
//! Every knob has a `RUSTLE_*` env var and a default, so the server runs with zero
//! configuration but is fully tunable in production without recompiling.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    /// Address to bind, e.g. "0.0.0.0:3000".
    pub addr: String,
    /// Maximum size of a single chat message body, in bytes.
    pub max_message_bytes: usize,
    /// Maximum length (in characters) of a display name.
    pub max_name_len: usize,
    /// Maximum length (in characters) of a room name.
    pub max_room_len: usize,
    /// Per-room broadcast buffer: how many messages a slow client may fall behind.
    pub room_capacity: usize,
    /// Rate limit: max messages a single connection may send per `rate_limit_window`.
    pub rate_limit_max: u32,
    /// The rate-limit window.
    pub rate_limit_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:3000".to_owned(),
            max_message_bytes: 4096,
            max_name_len: 24,
            max_room_len: 24,
            room_capacity: 128,
            rate_limit_max: 10,
            rate_limit_window: Duration::from_secs(10),
        }
    }
}

impl Config {
    /// Build a config from the environment, falling back to [`Config::default`] per field.
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            addr: env_or("RUSTLE_ADDR", d.addr),
            max_message_bytes: env_or("RUSTLE_MAX_MESSAGE_BYTES", d.max_message_bytes),
            max_name_len: env_or("RUSTLE_MAX_NAME_LEN", d.max_name_len),
            max_room_len: env_or("RUSTLE_MAX_ROOM_LEN", d.max_room_len),
            room_capacity: env_or("RUSTLE_ROOM_CAPACITY", d.room_capacity),
            rate_limit_max: env_or("RUSTLE_RATE_LIMIT_MAX", d.rate_limit_max),
            rate_limit_window: Duration::from_secs(env_or(
                "RUSTLE_RATE_LIMIT_WINDOW_SECS",
                d.rate_limit_window.as_secs(),
            )),
        }
    }
}

/// Read an env var and parse it, or return `default` if unset/unparseable.
fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
