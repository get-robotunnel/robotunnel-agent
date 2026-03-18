//! Heartbeat service — periodically reports agent status to the platform.

use std::fs;
use std::path::Path;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    network_profile: Option<NetworkProfilePayload>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct NetworkProfilePayload {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    private_addrs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_ip_hint: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    transport_caps: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    network_tags: Vec<String>,
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
            network_profile: build_network_profile(local_ip),
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

fn build_network_profile(local_ip: Option<&str>) -> Option<NetworkProfilePayload> {
    let mut private_addrs = Vec::new();
    if let Some(ip) = local_ip.map(str::trim).filter(|s| !s.is_empty()) {
        private_addrs.push(ip.to_string());
    }
    let network_tags = detect_network_tags(local_ip);

    Some(NetworkProfilePayload {
        private_addrs,
        public_ip_hint: None,
        transport_caps: vec![
            "lan_tcp".to_string(),
            "public_tcp".to_string(),
            "stun_p2p".to_string(),
            "turn_relay".to_string(),
            "vps_relay".to_string(),
        ],
        network_tags,
    })
}

fn detect_network_tags(local_ip: Option<&str>) -> Vec<String> {
    let mut tags = Vec::new();
    let docker_env = Path::new("/.dockerenv").exists();
    let cgroup = fs::read_to_string("/proc/1/cgroup")
        .unwrap_or_default()
        .to_lowercase();
    let docker_cgroup = cgroup.contains("docker");
    let container_cgroup = docker_cgroup
        || cgroup.contains("containerd")
        || cgroup.contains("kubepods")
        || cgroup.contains("libpod");

    if docker_env || docker_cgroup {
        push_tag(&mut tags, "runtime:docker");
        push_tag(&mut tags, "runtime:container");
    } else if container_cgroup {
        push_tag(&mut tags, "runtime:container");
    }

    if tags.iter().any(|tag| tag == "runtime:container") {
        push_tag(&mut tags, "network:containerized");
    }

    if tags.iter().any(|tag| tag == "runtime:docker")
        && local_ip
            .map(str::trim)
            .filter(|ip| !ip.is_empty())
            .is_some_and(is_likely_docker_bridge_ipv4)
    {
        push_tag(&mut tags, "network:docker_bridge");
        push_tag(&mut tags, "network:container_nat");
    }

    tags
}

fn push_tag(tags: &mut Vec<String>, tag: &str) {
    if tags.iter().any(|existing| existing == tag) {
        return;
    }
    tags.push(tag.to_string());
}

fn is_likely_docker_bridge_ipv4(ip: &str) -> bool {
    let parsed = match ip.trim().parse::<std::net::Ipv4Addr>() {
        Ok(ip) => ip,
        Err(_) => return false,
    };
    let octets = parsed.octets();
    octets[0] == 172 && (17..=31).contains(&octets[1])
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
            network_profile: build_network_profile(Some("192.168.1.100")),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("rob_test123"));
        assert!(json.contains("192.168.1.100"));
        assert!(json.contains("network_profile"));
    }

    #[test]
    fn test_heartbeat_payload_without_ip() {
        let payload = HeartbeatPayload {
            api_key: "rob_test".to_string(),
            local_ip: None,
            network_profile: build_network_profile(None),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(!json.contains("\"local_ip\""));
        assert!(json.contains("network_profile"));
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
