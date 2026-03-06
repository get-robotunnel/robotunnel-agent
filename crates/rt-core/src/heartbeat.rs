//! Heartbeat service — periodically reports agent status to the platform.

use std::time::Duration;
use tokio::sync::watch;
use tracing;

/// Heartbeat reporter that sends periodic POSTs to the platform API.
pub struct HeartbeatService {
    api_url: String,
    api_key: String,
    interval: Duration,
    client: reqwest::Client,
}

#[derive(Debug, serde::Serialize)]
struct HeartbeatPayload {
    api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_ip: Option<String>,
}

impl HeartbeatService {
    pub fn new(api_url: String, api_key: String, interval_secs: u64) -> Self {
        Self {
            api_url,
            api_key,
            interval: Duration::from_secs(interval_secs),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build HTTP client"),
        }
    }

    /// Run the heartbeat loop until shutdown signal is received.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        tracing::info!(
            "heartbeat: starting (interval={}s, url={})",
            self.interval.as_secs(),
            self.api_url
        );

        let mut interval = tokio::time::interval(self.interval);
        let local_ip = detect_local_ip();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.send_heartbeat(local_ip.as_deref()).await;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("heartbeat: shutdown signal received");
                        break;
                    }
                }
            }
        }
    }

    async fn send_heartbeat(&self, local_ip: Option<&str>) {
        let url = format!("{}/api/heartbeat", self.api_url);
        let payload = HeartbeatPayload {
            api_key: self.api_key.clone(),
            local_ip: local_ip.map(|s| s.to_string()),
        };

        match self.client.post(&url).json(&payload).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    tracing::debug!("heartbeat: sent successfully");
                } else {
                    tracing::warn!("heartbeat: server returned {}", resp.status());
                }
            }
            Err(e) => {
                tracing::warn!("heartbeat: failed to send: {}", e);
            }
        }
    }
}

/// Best-effort local IP detection.
fn detect_local_ip() -> Option<String> {
    use std::net::UdpSocket;
    // Connect to a public address to determine the local IP
    // (doesn't actually send any data)
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let addr = socket.local_addr().ok()?;
    Some(addr.ip().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_payload_serialization() {
        let payload = HeartbeatPayload {
            api_key: "rob_test123".to_string(),
            local_ip: Some("192.168.1.100".to_string()),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("rob_test123"));
        assert!(json.contains("192.168.1.100"));
    }

    #[test]
    fn test_heartbeat_payload_without_ip() {
        let payload = HeartbeatPayload {
            api_key: "rob_test".to_string(),
            local_ip: None,
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("local_ip"));
    }

    #[test]
    fn test_detect_local_ip() {
        // This test may fail in some CI/sandbox environments
        let ip = detect_local_ip();
        // Just check it doesn't panic; actual IP depends on network
        if let Some(ip) = ip {
            assert!(!ip.is_empty());
        }
    }
}
