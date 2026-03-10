use crate::tunnel::TunnelServer;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

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
        let url = format!(
            "{}/api/agent/authorized-keys?api_key={}",
            self.api_url.trim_end_matches('/'),
            self.api_key
        );

        match self.client.get(&url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp) => match resp.json::<AuthorizedKeysResponse>().await {
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
                        tracing::warn!("authorized_keys: parse failed: {}", err);
                    }
                },
                Err(err) => {
                    tracing::warn!("authorized_keys: server returned error: {}", err);
                }
            },
            Err(err) => {
                tracing::warn!("authorized_keys: sync failed: {}", err);
            }
        }
    }
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
