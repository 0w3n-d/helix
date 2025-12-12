use bytes::Bytes;
use helix_common::api::builder_api::TopBidUpdate;
use tokio::sync::broadcast;
use tracing::{error, warn};

/// Spawns a background task that reads TopBidUpdate from source channel,
/// encodes to both SSZ and JSON, and publishes to respective channels.
pub fn spawn_top_bid_encoder(
    source_tx: broadcast::Sender<TopBidUpdate>,
    ssz_tx: broadcast::Sender<Bytes>,
    json_tx: broadcast::Sender<String>,
) {
    tokio::spawn(async move {
        let mut source_rx = source_tx.subscribe();
        loop {
            match source_rx.recv().await {
                Ok(top_bid) => {
                    // Encode SSZ (off the hot path)
                    if let Err(e) = ssz_tx.send(top_bid.as_ssz_bytes_fast().into()) {
                        warn!("No SSZ WebSocket subscribers: {}", e);
                    }

                    // Encode JSON (off the hot path)
                    match serde_json::to_string(&top_bid) {
                        Ok(json_str) => {
                            if let Err(e) = json_tx.send(json_str) {
                                warn!("No JSON WebSocket subscribers: {}", e);
                            }
                        }
                        Err(e) => error!("Failed to JSON encode TopBid: {}", e),
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("Top bid encoder lagged, skipped {} messages", skipped);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    error!("Top bid source channel closed, stopping encoder");
                    break;
                }
            }
        }
    });
}
