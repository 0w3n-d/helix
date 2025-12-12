use std::sync::Arc;

use axum::{Extension, http::StatusCode, response::IntoResponse};
use helix_common::{RelayConfig, api::builder_api::TopBidUpdate, local_cache::LocalCache};

use crate::{
    api::Api, auctioneer::AuctioneerHandle,
    database::postgres::postgres_db_service::PostgresDatabaseService, housekeeper::CurrentSlotInfo,
};

pub(crate) const MAX_PAYLOAD_LENGTH: usize = 1024 * 1024 * 20; // 20MB

#[derive(Clone)]
pub struct BuilderApi<A: Api> {
    pub local_cache: Arc<LocalCache>,
    pub db: Arc<PostgresDatabaseService>,
    pub curr_slot_info: CurrentSlotInfo,
    pub relay_config: Arc<RelayConfig>,
    /// Subscriber for TopBid updates
    pub top_bid_tx: tokio::sync::broadcast::Sender<TopBidUpdate>,
    pub top_bid_ssz_tx: tokio::sync::broadcast::Sender<bytes::Bytes>,
    pub auctioneer_handle: AuctioneerHandle,
    pub api_provider: Arc<A::ApiProvider>,
}

impl<A: Api> BuilderApi<A> {
    pub fn new(
        local_cache: Arc<LocalCache>,
        db: Arc<PostgresDatabaseService>,
        relay_config: RelayConfig,
        curr_slot_info: CurrentSlotInfo,
        top_bid_tx: tokio::sync::broadcast::Sender<TopBidUpdate>,
        top_bid_ssz_tx: tokio::sync::broadcast::Sender<bytes::Bytes>,
        auctioneer_handle: AuctioneerHandle,
        api_provider: Arc<A::ApiProvider>,
    ) -> Self {
        Self {
            local_cache,
            db,
            relay_config: Arc::new(relay_config),
            curr_slot_info,
            top_bid_tx,
            top_bid_ssz_tx,
            auctioneer_handle,
            api_provider,
        }
    }

    /// Implements this API: <https://flashbots.github.io/relay-specs/#/Builder/getValidators>
    pub async fn get_validators(
        Extension(api): Extension<Arc<BuilderApi<A>>>,
    ) -> impl IntoResponse {
        if let Some(duty_bytes) = api.curr_slot_info.proposer_duties_response() {
            (StatusCode::OK, duty_bytes.0).into_response()
        } else {
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
