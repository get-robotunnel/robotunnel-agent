//! rt-webrtc — WebRTC P2P connectivity for RoboTunnel Agent.
//!
//! # Connection Strategy
//!
//! ```text
//! Phase 1: STUN (direct ICE P2P)
//!   → If ICE gathering completes within 5s and a host/srflx candidate pair
//!     is nominated, use the direct path. Zero relay bandwidth cost.
//!
//! Phase 2: TURN relay (fallback)
//!   → If STUN-only ICE fails (no candidate pair within timeout), fetch
//!     short-lived TURN credentials from the platform and retry.
//!   → TURN relay is bandwidth-metered on VPS; only used when necessary.
//!
//! Phase 3: TCP tunnel (last resort)
//!   → If WebRTC fails entirely (both STUN and TURN), fall back to the
//!     existing TCP tunnel mechanism. Connection degrades gracefully.
//! ```
//!
//! # Signaling Protocol
//! Agent connects to `wss://platform/api/signal/{robot_id}?role=agent&token=...`
//! Messages: JSON `SignalMessage` structs (type: offer/answer/ice-candidate/bye)

pub mod client;
pub mod types;

use tracing::info;

/// WebRTC connection state reported to the caller.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionType {
    /// Direct P2P via STUN — preferred, zero relay cost.
    Stun,
    /// Relayed via TURN server — fallback when STUN fails.
    Turn,
    /// TCP tunnel fallback — used when WebRTC fails entirely.
    TcpTunnel,
}

impl ConnectionType {
    pub fn display(&self) -> &'static str {
        match self {
            ConnectionType::Stun      => "STUN/P2P (direct)",
            ConnectionType::Turn      => "TURN (relayed)",
            ConnectionType::TcpTunnel => "TCP tunnel (fallback)",
        }
    }
}

/// Configuration for the WebRTC client.
#[derive(Clone, Debug)]
pub struct WebRtcConfig {
    /// Platform signaling WebSocket URL, e.g. wss://api.robotunnel.io
    pub platform_url: String,
    /// Robot ID for session addressing
    pub robot_id: String,
    /// Platform API token
    pub token: String,
    /// ICE gathering timeout before falling back to TURN (seconds)
    pub stun_timeout_secs: u64,
}

impl WebRtcConfig {
    pub fn signaling_url(&self) -> String {
        format!(
            "{}/api/signal/{}?role=agent&token={}",
            self.platform_url.trim_end_matches('/'),
            self.robot_id,
            self.token
        )
    }

    pub fn turn_credentials_url(&self) -> String {
        format!(
            "{}/api/turn-credentials?token={}&robot_id={}",
            self.platform_url.trim_end_matches('/'),
            self.token,
            self.robot_id
        )
    }
}

/// Convenience: log and report connection type.
pub fn log_connection_type(conn_type: &ConnectionType) {
    info!(
        "WebRTC connection established via {}",
        conn_type.display()
    );
}
