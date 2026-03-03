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
