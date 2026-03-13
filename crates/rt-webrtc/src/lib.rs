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
//! Agent connects to `wss://platform/api/signal/{robot_id}?role=agent`
//! with `X-Robot-API-Key` header.
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
            ConnectionType::Stun => "STUN/P2P (direct)",
            ConnectionType::Turn => "TURN (relayed)",
            ConnectionType::TcpTunnel => "TCP tunnel (fallback)",
        }
    }
}

/// Configuration for the WebRTC client.
#[derive(Clone, Debug)]
pub struct WebRtcConfig {
    /// Platform base URL, typically `https://api.robotunnel.io`.
    /// Also accepts `ws://` / `wss://`; methods normalize as needed.
    pub platform_url: String,
    /// Robot ID for session addressing
    pub robot_id: String,
    /// Robot API key issued by platform register API
    pub api_key: String,
    /// ICE gathering timeout before falling back to TURN (seconds)
    pub stun_timeout_secs: u64,
    /// Optional trace ID to correlate logs across Agent/Platform/CLI
    pub bootstrap_id: Option<String>,
}

impl WebRtcConfig {
    pub fn signaling_url(&self) -> String {
        let mut url = format!(
            "{}/api/signal/{}?role=agent",
            self.signaling_base_url(),
            self.robot_id
        );
        if let Some(id) = &self.bootstrap_id {
            url.push_str(&format!("&bootstrap_id={}", id));
        }
        url
    }

    pub fn turn_credentials_url(&self) -> String {
        let mut url = format!(
            "{}/api/turn-credentials?robot_id={}",
            self.http_base_url(),
            self.robot_id
        );
        if let Some(id) = &self.bootstrap_id {
            url.push_str(&format!("&bootstrap_id={}", id));
        }
        url
    }

    fn signaling_base_url(&self) -> String {
        let trimmed = self.platform_url.trim_end_matches('/');
        if let Some(rest) = trimmed.strip_prefix("https://") {
            return format!("wss://{}", rest);
        }
        if let Some(rest) = trimmed.strip_prefix("http://") {
            return format!("ws://{}", rest);
        }
        trimmed.to_string()
    }

    fn http_base_url(&self) -> String {
        let trimmed = self.platform_url.trim_end_matches('/');
        if let Some(rest) = trimmed.strip_prefix("wss://") {
            return format!("https://{}", rest);
        }
        if let Some(rest) = trimmed.strip_prefix("ws://") {
            return format!("http://{}", rest);
        }
        trimmed.to_string()
    }
}

/// Convenience: log and report connection type.
pub fn log_connection_type(conn_type: &ConnectionType) {
    info!("WebRTC connection established via {}", conn_type.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cfg(platform_url: &str) -> WebRtcConfig {
        WebRtcConfig {
            platform_url: platform_url.to_string(),
            robot_id: "robot-123".to_string(),
            api_key: "rob_test".to_string(),
            stun_timeout_secs: 8,
            bootstrap_id: None,
        }
    }

    #[test]
    fn test_url_build_from_https_base() {
        let cfg = make_cfg("https://api.robotunnel.io/");
        assert_eq!(
            cfg.signaling_url(),
            "wss://api.robotunnel.io/api/signal/robot-123?role=agent"
        );
        assert_eq!(
            cfg.turn_credentials_url(),
            "https://api.robotunnel.io/api/turn-credentials?robot_id=robot-123"
        );
    }

    #[test]
    fn test_url_build_from_wss_base() {
        let cfg = make_cfg("wss://api.robotunnel.io");
        assert_eq!(
            cfg.signaling_url(),
            "wss://api.robotunnel.io/api/signal/robot-123?role=agent"
        );
        assert_eq!(
            cfg.turn_credentials_url(),
            "https://api.robotunnel.io/api/turn-credentials?robot_id=robot-123"
        );
    }
}
