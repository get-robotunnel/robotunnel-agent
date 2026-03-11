//! Signal and ICE data types for WebRTC signaling protocol.

use serde::{Deserialize, Serialize};

/// JSON message exchanged over the signaling WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalMessage {
    /// "offer" | "answer" | "ice-candidate" | "client-ready" | "bye"
    pub r#type: String,
    /// SDP string (for offer/answer) or ICE candidate JSON (for ice-candidate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    pub robot_id: String,
    /// Optional trace ID to correlate logs across Agent/Platform/CLI
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_id: Option<String>,
}

/// Phases of the WebRTC bootstrap process for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapPhase {
    StunStart,
    StunTimeout,
    StunConnected,
    TurnFetchStart,
    TurnFetchOk,
    TurnFetchFail,
    SignalWsConnectStart,
    SignalWsConnectOk,
    SignalWsConnectFail,
    OfferSent,
    AnswerReceived,
    DataChannelOpen,
}

impl BootstrapPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StunStart => "STUN_START",
            Self::StunTimeout => "STUN_TIMEOUT",
            Self::StunConnected => "STUN_CONNECTED",
            Self::TurnFetchStart => "TURN_FETCH_START",
            Self::TurnFetchOk => "TURN_FETCH_OK",
            Self::TurnFetchFail => "TURN_FETCH_FAIL",
            Self::SignalWsConnectStart => "SIGNAL_WS_CONNECT_START",
            Self::SignalWsConnectOk => "SIGNAL_WS_CONNECT_OK",
            Self::SignalWsConnectFail => "SIGNAL_WS_CONNECT_FAIL",
            Self::OfferSent => "OFFER_SENT",
            Self::AnswerReceived => "ANSWER_RECEIVED",
            Self::DataChannelOpen => "DATACHANNEL_OPEN",
        }
    }
}

/// TURN credential response from /api/turn-credentials
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCredentialResponse {
    pub turn_available: bool,
    #[serde(default)]
    pub stun_urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCredentials {
    pub urls: Vec<String>,
    pub username: String,
    pub credential: String,
    pub ttl_seconds: u32,
}
