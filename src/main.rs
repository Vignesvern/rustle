//! Binary entrypoint: initialise logging, read config, build the app, and serve.

use rustle::{AppState, build_app, config::Config};

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
    let app = build_app(AppState::new(config));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("rustle listening on {addr}");

    axum::serve(listener, app).await?;
    Ok(())
}
