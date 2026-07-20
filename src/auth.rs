//! Authentication primitives: argon2 password hashing and HS256 JWTs.
//!
//! Passwords are never stored in the clear — only their argon2 hash (which embeds a
//! random salt). On login we verify the presented password against the stored hash and,
//! on success, issue a short-lived JWT the client presents when opening a WebSocket.

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};

/// JWT claims: `sub` is the username, `exp` a Unix expiry timestamp.
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

/// Hash a password with argon2 (random salt). Returns the PHC-format hash string.
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

/// Verify a password against a stored argon2 hash. Returns false on any parse/verify error.
pub fn verify_password(password: &str, hash: &str) -> bool {
    match PasswordHash::new(hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

/// Issue a signed JWT for `username`, valid for `ttl_secs` seconds.
pub fn issue_token(
    secret: &str,
    username: &str,
    ttl_secs: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let exp = (chrono::Utc::now().timestamp() as u64).saturating_add(ttl_secs) as usize;
    let claims = Claims {
        sub: username.to_owned(),
        exp,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// Verify a token and return the username (`sub`) if valid and unexpired.
pub fn verify_token(secret: &str, token: &str) -> Option<String> {
    let validation = Validation::new(Algorithm::HS256);
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()
    .map(|data| data.claims.sub)
}
