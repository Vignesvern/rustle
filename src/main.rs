//! Binary entrypoint: initialise logging, read config, connect the DB (if configured),
//! build the app, and serve.

use rustle::{AppState, build_app, config::Config, db};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustle=info,tower_http=info".into()),
        )
        .init();

    let config = Config::from_env();
    let addr = config.addr.clone();

    let db = match &config.database_url {
        Some(url) => {
            tracing::info!("connecting to Postgres for persistence");
            Some(db::init(url).await?)
        }
        None => {
            tracing::warn!("DATABASE_URL not set — running without persistence");
            None
        }
    };

    let app = build_app(AppState::new(config).with_db(db));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("rustle listening on {addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
