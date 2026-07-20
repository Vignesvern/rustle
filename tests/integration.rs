//! End-to-end integration tests.
//!
//! WebSocket tests spin up a real server on an ephemeral port and drive it with real
//! `tokio-tungstenite` clients. HTTP tests use axum's `oneshot` to call the router
//! directly (no socket needed).

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use rustle::config::Config;
use rustle::{AppState, build_app};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Start the server on a random free port and return its address.
async fn spawn(config: Config) -> SocketAddr {
    spawn_state(AppState::new(config)).await
}

/// Start the server from a prebuilt state (e.g. one carrying a database pool).
async fn spawn_state(state: AppState) -> SocketAddr {
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Connect a client and send its join frame.
async fn join(addr: SocketAddr, room: &str, name: &str) -> Ws {
    let (mut ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    let frame = json!({ "type": "join", "room": room, "name": name }).to_string();
    ws.send(WsMessage::Text(frame.into())).await.unwrap();
    ws
}

async fn send_msg(ws: &mut Ws, body: &str) {
    let frame = json!({ "type": "message", "body": body }).to_string();
    ws.send(WsMessage::Text(frame.into())).await.unwrap();
}

/// Read frames until `pred` matches (returns it) or a short timeout elapses (returns None).
async fn recv_until<F: Fn(&Value) -> bool>(ws: &mut Ws, pred: F) -> Option<Value> {
    loop {
        match tokio::time::timeout(Duration::from_millis(800), ws.next()).await {
            Ok(Some(Ok(WsMessage::Text(t)))) => {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                if pred(&v) {
                    return Some(v);
                }
            }
            Ok(Some(Ok(_))) => continue, // ignore ping/pong/close
            _ => return None,            // timeout, error, or stream end
        }
    }
}

/// Collect all frames arriving within a short window.
async fn drain(ws: &mut Ws) -> Vec<Value> {
    let mut out = Vec::new();
    while let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_millis(400), ws.next()).await
    {
        if let WsMessage::Text(t) = msg {
            out.push(serde_json::from_str(t.as_str()).unwrap());
        }
    }
    out
}

fn is_msg(v: &Value, body: &str) -> bool {
    v["type"] == "message" && v["body"] == body
}

#[tokio::test]
async fn broadcast_within_room() {
    let addr = spawn(Config::default()).await;
    let mut alice = join(addr, "general", "alice").await;
    let mut bob = join(addr, "general", "bob").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain(&mut alice).await;
    drain(&mut bob).await;

    send_msg(&mut alice, "hello").await;
    let got = recv_until(&mut bob, |m| is_msg(m, "hello"))
        .await
        .expect("bob should receive it");
    assert_eq!(got["name"], "alice");
    assert!(got["ts"].is_string());
}

#[tokio::test]
async fn messages_are_isolated_per_room() {
    let addr = spawn(Config::default()).await;
    let mut alice = join(addr, "general", "alice").await;
    let mut carol = join(addr, "random", "carol").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain(&mut alice).await;
    drain(&mut carol).await;

    send_msg(&mut alice, "just for general").await;
    let leaked = recv_until(&mut carol, |m| is_msg(m, "just for general")).await;
    assert!(leaked.is_none(), "message leaked across rooms");
}

#[tokio::test]
async fn presence_updates_on_join_and_leave() {
    let addr = spawn(Config::default()).await;
    let mut alice = join(addr, "general", "alice").await;
    let mut bob = join(addr, "general", "bob").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain(&mut alice).await;

    // Carol joins -> alice sees a roster of three.
    let mut carol = join(addr, "general", "carol").await;
    let pres = recv_until(&mut alice, |m| {
        m["type"] == "presence" && m["users"].as_array().map(|a| a.len()) == Some(3)
    })
    .await;
    assert_eq!(
        pres.expect("roster of 3")["users"],
        json!(["alice", "bob", "carol"])
    );

    // Carol leaves -> alice sees a "left" notice and a roster of two.
    carol.close(None).await.unwrap();
    let left = recv_until(&mut alice, |m| {
        m["type"] == "system" && m["body"].as_str().unwrap_or("").contains("left")
    })
    .await;
    assert!(left.is_some(), "expected a 'left' notice");
    let pres = recv_until(&mut alice, |m| m["type"] == "presence").await;
    assert_eq!(
        pres.expect("roster after leave")["users"],
        json!(["alice", "bob"])
    );

    let _ = bob.close(None).await;
}

#[tokio::test]
async fn rate_limit_drops_excess_messages() {
    let config = Config {
        rate_limit_max: 3,
        rate_limit_window: Duration::from_secs(30),
        ..Config::default()
    };
    let addr = spawn(config).await;
    let mut alice = join(addr, "general", "alice").await;
    let mut bob = join(addr, "general", "bob").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain(&mut alice).await;
    drain(&mut bob).await;

    for i in 0..6 {
        send_msg(&mut alice, &format!("m{i}")).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let bob_msgs = drain(&mut bob).await;
    let delivered = bob_msgs.iter().filter(|m| m["type"] == "message").count();
    assert_eq!(delivered, 3, "rate limit should cap delivered messages");

    let alice_msgs = drain(&mut alice).await;
    let warned = alice_msgs
        .iter()
        .any(|m| m["type"] == "system" && m["body"].as_str().unwrap_or("").contains("too fast"));
    assert!(warned, "sender should be warned about rate limiting");
}

#[tokio::test]
async fn oversized_messages_are_rejected() {
    let config = Config {
        max_message_bytes: 8,
        ..Config::default()
    };
    let addr = spawn(config).await;
    let mut alice = join(addr, "general", "alice").await;
    let mut bob = join(addr, "general", "bob").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    drain(&mut alice).await;
    drain(&mut bob).await;

    send_msg(&mut alice, "this body is way longer than eight bytes").await;
    let leaked = recv_until(&mut bob, |m| m["type"] == "message").await;
    assert!(
        leaked.is_none(),
        "oversized message should not be broadcast"
    );

    let warned = recv_until(&mut alice, |m| {
        m["type"] == "system" && m["body"].as_str().unwrap_or("").contains("too long")
    })
    .await;
    assert!(warned.is_some(), "sender should be warned about oversize");
}

// --- HTTP endpoints via oneshot (no socket) ---------------------------------------

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::util::ServiceExt;

#[tokio::test]
async fn health_returns_ok() {
    let app = build_app(AppState::new(Config::default()));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn unknown_room_returns_404() {
    let app = build_app(AppState::new(Config::default()));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/rooms/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_rooms_reflects_membership() {
    let state = AppState::new(Config::default());
    let _ = state.hub.join("general", 1, "alice".to_owned());
    let _ = state.hub.join("general", 2, "bob".to_owned());
    let _ = state.hub.join("random", 3, "carol".to_owned());
    let app = build_app(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/rooms")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v[0]["name"], "general");
    assert_eq!(v[0]["count"], 2);
    assert_eq!(v[1]["name"], "random");
    assert_eq!(v[1]["count"], 1);
}

// --- Persistence (runs only when DATABASE_URL is set) ------------------------------

#[tokio::test]
async fn history_is_replayed_on_join() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping history_is_replayed_on_join: DATABASE_URL not set");
        return;
    };

    let pool = rustle::db::init(&url).await.expect("db init + migrate");
    let room = "hist-test-room";
    // Start from a clean slate for this room.
    sqlx::query("DELETE FROM messages WHERE room = $1")
        .bind(room)
        .execute(&pool)
        .await
        .unwrap();

    let state = AppState::new(Config::default()).with_db(Some(pool.clone()));
    let addr = spawn_state(state).await;

    // Alice joins, posts two messages, then leaves.
    let mut alice = join(addr, room, "alice").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    send_msg(&mut alice, "first").await;
    send_msg(&mut alice, "second").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    alice.close(None).await.unwrap();

    // Bob joins later and should receive both historical messages, in order.
    let mut bob = join(addr, room, "bob").await;
    let first = recv_until(&mut bob, |m| is_msg(m, "first")).await;
    let second = recv_until(&mut bob, |m| is_msg(m, "second")).await;
    assert!(first.is_some(), "history: 'first' should be replayed");
    assert!(second.is_some(), "history: 'second' should be replayed");
    assert_eq!(first.unwrap()["name"], "alice");

    let _ = bob.close(None).await;
    sqlx::query("DELETE FROM messages WHERE room = $1")
        .bind(room)
        .execute(&pool)
        .await
        .unwrap();
}

// --- Auth ---------------------------------------------------------------------------

use axum::Router;

fn json_post(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn call(app: Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, v)
}

#[tokio::test]
async fn invalid_token_is_rejected() {
    // Token verification is stateless, so this needs no database.
    let addr = spawn(Config::default()).await;
    let res = connect_async(format!("ws://{addr}/ws?token=not-a-real-token")).await;
    assert!(
        res.is_err(),
        "an invalid token must be rejected at the handshake"
    );
}

#[tokio::test]
async fn register_login_and_authenticated_identity() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping register_login_and_authenticated_identity: DATABASE_URL not set");
        return;
    };
    let pool = rustle::db::init(&url).await.expect("db init");
    let uname = "m5_alice";
    let room = "m5-room";
    sqlx::query("DELETE FROM users WHERE username = $1")
        .bind(uname)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM messages WHERE room = $1")
        .bind(room)
        .execute(&pool)
        .await
        .unwrap();

    let state = AppState::new(Config::default()).with_db(Some(pool.clone()));

    // Register -> 200 + token.
    let (st, body) = call(
        build_app(state.clone()),
        json_post(
            "/api/register",
            json!({"username": uname, "password": "secret1"}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["username"], uname);
    assert!(body["token"].as_str().is_some());

    // Registering the same name again -> 409.
    let (st, _) = call(
        build_app(state.clone()),
        json_post(
            "/api/register",
            json!({"username": uname, "password": "secret1"}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);

    // Wrong password -> 401.
    let (st, _) = call(
        build_app(state.clone()),
        json_post(
            "/api/login",
            json!({"username": uname, "password": "WRONG"}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // Correct login -> token.
    let (st, body) = call(
        build_app(state.clone()),
        json_post(
            "/api/login",
            json!({"username": uname, "password": "secret1"}),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let token = body["token"].as_str().unwrap().to_owned();

    // An authenticated client's identity comes from the token, not the client-sent name.
    let addr = spawn_state(state).await;
    let (mut alice, _) = connect_async(format!("ws://{addr}/ws?token={token}"))
        .await
        .unwrap();
    alice
        .send(WsMessage::Text(
            json!({"type": "join", "room": room, "name": "IMPOSTER"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    let mut bob = join(addr, room, "bob").await; // guest
    tokio::time::sleep(Duration::from_millis(150)).await;
    drain(&mut bob).await;

    alice
        .send(WsMessage::Text(
            json!({"type": "message", "body": "hi"}).to_string().into(),
        ))
        .await
        .unwrap();
    let got = recv_until(&mut bob, |m| is_msg(m, "hi"))
        .await
        .expect("bob should receive the message");
    assert_eq!(
        got["name"], uname,
        "authenticated identity must come from the token, not the client"
    );

    let _ = alice.close(None).await;
    let _ = bob.close(None).await;
    sqlx::query("DELETE FROM users WHERE username = $1")
        .bind(uname)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM messages WHERE room = $1")
        .bind(room)
        .execute(&pool)
        .await
        .unwrap();
}
