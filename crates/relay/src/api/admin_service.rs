use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{Extension, Router, extract::{WebSocketUpgrade, ws::{Message, WebSocket}}, http::StatusCode, response::{IntoResponse, Response}, routing::{get, post}};
use bytes::Bytes;
use futures::StreamExt;
use helix_common::{RelayConfig, local_cache::LocalCache};
use http::HeaderMap;
use tokio::time;
use tower_http::validate_request::ValidateRequestHeaderLayer;
use tracing::{debug, error, info};

#[derive(Clone)]
struct AdminService {
    auctioneer: Arc<LocalCache>,
    config: RelayConfig,
    top_bid_tx_js: tokio::sync::broadcast::Sender<String>,
}

pub async fn run_admin_service(
    auctioneer: Arc<LocalCache>,
    config: RelayConfig,
    top_bid_tx_js: tokio::sync::broadcast::Sender<String>,
) {
    let admin_service = AdminService {
        auctioneer,
        config: config.clone(),
        top_bid_tx_js,
    };

    let rest = Router::new()
    .route(
        "/admin/v1/killswitch",
        post(enable_kill_switch).delete(disable_kill_switch),
    )
    .layer(Extension(admin_service.clone()))
    .route_layer(ValidateRequestHeaderLayer::bearer(&config.admin_token));

    let ws = Router::new()
        .route("/admin/v1/top_bid", get(get_top_bid))
        .layer(Extension(admin_service));

    let router = Router::new()
        .merge(rest)
        .merge(ws);


    let listener = tokio::net::TcpListener::bind("0.0.0.0:4050").await.unwrap();
    match axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await {
        Ok(_) => info!("Server exited successfully"),
        Err(e) => error!("Server exited with error: {e}"),
    }
}

async fn enable_kill_switch(
    Extension(admin_service): Extension<Arc<AdminService>>,
) -> Result<impl IntoResponse, StatusCode> {
    admin_service.auctioneer.enable_kill_switch();
    info!("Kill switch enabled");
    Ok((StatusCode::NO_CONTENT, ()))
}

async fn disable_kill_switch(
    Extension(admin_service): Extension<Arc<AdminService>>,
) -> Result<impl IntoResponse, StatusCode> {
    admin_service.auctioneer.disable_kill_switch();
    info!("Kill switch disabled");
    Ok((StatusCode::NO_CONTENT, ()))
}

#[tracing::instrument(skip_all)]
async fn get_top_bid(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Extension(admin_service): Extension<Arc<AdminService>>,
) -> Response {
    info!("Admin WebSocket connection attempt");
    if let Some(protocol) = headers.get("sec-websocket-protocol") {
        if let Ok(protocol_str) = protocol.to_str() {
            if let Some(token) = protocol_str.strip_prefix("bearer.") {
                if token == admin_service.config.admin_token {
                    return ws
                        .protocols(["bearer"])
                        .on_upgrade(move |socket| {
                            let sub = admin_service.top_bid_tx_js.subscribe();
                            push_top_bids(socket, sub)
                        });
                }
            }
        }
    }

    (StatusCode::UNAUTHORIZED, "Invalid or missing bearer token").into_response()
}



async fn push_top_bids(
    mut socket: WebSocket,
    mut bid_stream: tokio::sync::broadcast::Receiver<String>,
) {
    let mut interval = time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            Ok(bid) = bid_stream.recv() => {
                if socket.send(Message::Text(bid.into())).await.is_err() {
                    error!("Failed to send bid. Disconnecting.");
                    break;
                }
            },

            _ = interval.tick() => {
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    error!("Failed to send ping.");
                    break;
                }
            },

            msg = socket.next() => {
                match msg {
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            error!("Failed to respond to ping.");
                            break;
                        }
                    },
                    Some(Ok(Message::Pong(_))) => {
                        debug!("Received pong response.");
                    },
                    Some(Ok(Message::Close(_))) => {
                        debug!("Received close frame.");
                        break;
                    },
                    Some(Ok(Message::Binary(_))) => {
                        debug!("Received Binary frame.");
                    },
                    Some(Ok(Message::Text(_))) => {
                        debug!("Received Text frame.");
                    },
                    Some(Err(e)) => {
                        error!("Error in WebSocket connection: {}", e);
                        break;
                    },
                    None => {
                        error!("WebSocket connection closed by the other side.");
                        break;
                    }
                }
            }
        }
    }

    debug!("Socket connection closed gracefully.");
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod test {
    use std::sync::Arc;

    use helix_common::{config, local_cache::LocalCache};
    use serial_test::serial;

    use crate::api::admin_service::run_admin_service;

    #[tokio::test]
    #[serial]
    async fn test_admin_service() {
        let auctioneer = Arc::new(LocalCache::new_test());

        let mut config = config::RelayConfig::empty_for_test();
        config.admin_token = "test_token".into();
        tokio::spawn(run_admin_service(auctioneer.clone(), config, tokio::sync::broadcast::channel(100).0));
        tokio::time::sleep(std::time::Duration::from_secs(1)).await; // wait for server to start
        let client = reqwest::Client::new();

        let response = client
            .post("http://localhost:4050/admin/v1/killswitch")
            .bearer_auth("test_token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 204);
        assert!(auctioneer.kill_switch_enabled());

        let response = client
            .delete("http://localhost:4050/admin/v1/killswitch")
            .bearer_auth("test_token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 204);
        assert!(!auctioneer.kill_switch_enabled());
    }

    #[tokio::test]
    #[serial]
    async fn test_admin_service_unauthorized() {
        let auctioneer = Arc::new(LocalCache::new_test());

        let mut config = config::RelayConfig::empty_for_test();
        config.admin_token = "test_token".into();
        tokio::spawn(run_admin_service(auctioneer.clone(), config, tokio::sync::broadcast::channel(100).0));
        tokio::time::sleep(std::time::Duration::from_secs(1)).await; // wait for server to start
        let client = reqwest::Client::new();

        let response =
            client.get("http://localhost:4050/admin/v1/killswitch/enable").send().await.unwrap();
        assert_eq!(response.status(), 401);
        assert!(!auctioneer.kill_switch_enabled());

        let response =
            client.get("http://localhost:4050/admin/v1/killswitch/disable").send().await.unwrap();
        assert_eq!(response.status(), 401);
        assert!(!auctioneer.kill_switch_enabled());
    }
}
