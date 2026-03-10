use crate::tunnel::TunnelServer;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct AgentBootstrap {
    pub robot_id: Option<String>,
    pub authorized_keys: Vec<String>,
}

pub struct AuthorizedKeysSyncService {
    api_url: String,
    api_key: String,
    interval: Duration,
    static_authorized_keys: Vec<String>,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct AuthorizedKeysResponse {
    #[serde(default)]
    robot_id: Option<String>,
    #[serde(default)]
    authorized_keys: Vec<String>,
}

impl AuthorizedKeysSyncService {
    pub fn new(
        api_url: String,
        api_key: String,
        interval_secs: u64,
        static_authorized_keys: Vec<String>,
    ) -> Self {
        Self {
            api_url,
            api_key,
            interval: Duration::from_secs(interval_secs.max(15)),
            static_authorized_keys: normalize_keys(static_authorized_keys),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build HTTP client"),
        }
    }

    pub async fn run(&self, tunnel_server: Arc<TunnelServer>, mut shutdown: watch::Receiver<bool>) {
        tracing::info!(
            "authorized_keys: starting sync (interval={}s, url={})",
            self.interval.as_secs(),
            self.api_url
        );

        self.sync_once(&tunnel_server).await;
        let mut interval = tokio::time::interval(self.interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.sync_once(&tunnel_server).await;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("authorized_keys: shutdown signal received");
                        break;
                    }
                }
            }
        }
    }

    async fn sync_once(&self, tunnel_server: &Arc<TunnelServer>) {
        match fetch_agent_bootstrap_with_client(&self.client, &self.api_url, &self.api_key).await {
            Ok(payload) => {
                        let mut merged_keys = self.static_authorized_keys.clone();
                        merged_keys.extend(payload.authorized_keys);
                        let merged_keys = normalize_keys(merged_keys);
                        let current = tunnel_server.authorized_keys();
                        if current != merged_keys {
                            let count = merged_keys.len();
                            tunnel_server.replace_authorized_keys(merged_keys);
                            tracing::info!(
                                "authorized_keys: updated tunnel allowlist ({} key(s))",
                                count
                            );
                        } else {
                            tracing::debug!("authorized_keys: allowlist unchanged");
                        }
            }
            Err(err) => {
                tracing::warn!("authorized_keys: sync failed: {}", err);
            }
        }
    }
}

pub async fn fetch_agent_bootstrap(
    api_url: &str,
    api_key: &str,
) -> Result<AgentBootstrap, reqwest::Error> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    fetch_agent_bootstrap_with_client(&client, api_url, api_key).await
}

async fn fetch_agent_bootstrap_with_client(
    client: &reqwest::Client,
    api_url: &str,
    api_key: &str,
) -> Result<AgentBootstrap, reqwest::Error> {
    let url = format!(
        "{}/api/agent/authorized-keys?api_key={}",
        api_url.trim_end_matches('/'),
        api_key
    );

    let payload = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json::<AuthorizedKeysResponse>()
        .await?;

    Ok(AgentBootstrap {
        robot_id: payload
            .robot_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        authorized_keys: payload.authorized_keys,
    })
}

fn normalize_keys(keys: Vec<String>) -> Vec<String> {
    let mut normalized = keys
        .into_iter()
        .map(|key| key.trim().to_lowercase())
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}
