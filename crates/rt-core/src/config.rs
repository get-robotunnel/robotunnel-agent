//! Configuration loading for the RoboTunnel agent.

use serde::Deserialize;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

/// Top-level agent configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub platform: PlatformConfig,
    #[serde(default)]
    pub webrtc: WebRtcConfig,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    /// Ed25519 public key allowlist (hex, 64 chars each).
    /// Required by default unless insecure_allow_any_client is enabled.
    #[serde(default)]
    pub authorized_keys: Vec<String>,
    /// Development-only override to accept any valid Ed25519 signature.
    #[serde(default)]
    pub insecure_allow_any_client: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlatformConfig {
    #[serde(default = "default_api_url")]
    pub api_url: String,
    /// Robot API key. Can also be set via RT_API_KEY env var.
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebRtcConfig {
    #[serde(default = "default_webrtc_enabled")]
    pub enabled: bool,
    pub robot_id: Option<String>,
    #[serde(default = "default_stun_timeout_secs")]
    pub stun_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatConfig {
    #[serde(default = "default_heartbeat_interval")]
    pub interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_listen_port() -> u16 {
    11411
}
fn default_api_url() -> String {
    "https://api.robotunnel.io".to_string()
}
fn default_heartbeat_interval() -> u64 {
    30
}
fn default_webrtc_enabled() -> bool {
    true
}
fn default_stun_timeout_secs() -> u64 {
    8
}
fn default_log_level() -> String {
    "info".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_port: default_listen_port(),
            authorized_keys: Vec::new(),
            insecure_allow_any_client: false,
        }
    }
}

impl Default for PlatformConfig {
    fn default() -> Self {
        Self {
            api_url: default_api_url(),
            api_key: None,
        }
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_heartbeat_interval(),
        }
    }
}

impl Default for WebRtcConfig {
    fn default() -> Self {
        Self {
            enabled: default_webrtc_enabled(),
            robot_id: None,
            stun_timeout_secs: default_stun_timeout_secs(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            platform: PlatformConfig::default(),
            webrtc: WebRtcConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl AgentConfig {
    /// Load config from a TOML file. Falls back to defaults for missing fields.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: AgentConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load config, with environment variable overrides.
    pub fn load_with_env(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let mut config = Self::load(path)?;
        config.apply_env_overrides();
        config.validate_security()?;
        Ok(config)
    }

    /// Apply environment variable overrides.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(key) = std::env::var("RT_API_KEY") {
            self.platform.api_key = Some(key);
        }
        if let Ok(url) = std::env::var("RT_API_URL") {
            self.platform.api_url = url;
        }
        if let Ok(port) = std::env::var("RT_LISTEN_PORT") {
            if let Ok(p) = port.parse() {
                self.server.listen_port = p;
            }
        }
        if let Ok(keys) = std::env::var("RT_AUTHORIZED_KEYS") {
            self.server.authorized_keys = keys
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Ok(v) = std::env::var("RT_INSECURE_ALLOW_ANY_CLIENT") {
            self.server.insecure_allow_any_client = parse_bool(&v);
        }
        if let Ok(level) = std::env::var("RT_LOG_LEVEL") {
            self.logging.level = level;
        }
        if let Ok(enabled) = std::env::var("RT_WEBRTC_ENABLED") {
            let v = enabled.to_lowercase();
            self.webrtc.enabled = matches!(v.as_str(), "1" | "true" | "yes" | "on");
        }
        if let Ok(robot_id) = std::env::var("RT_ROBOT_ID") {
            if !robot_id.trim().is_empty() {
                self.webrtc.robot_id = Some(robot_id);
            }
        }
        if let Ok(timeout) = std::env::var("RT_WEBRTC_STUN_TIMEOUT_SECS") {
            if let Ok(v) = timeout.parse() {
                self.webrtc.stun_timeout_secs = v;
            }
        }
    }

    pub fn validate_security(&mut self) -> Result<(), ConfigError> {
        self.server.authorized_keys = self
            .server
            .authorized_keys
            .iter()
            .map(|key| key.trim().to_lowercase())
            .filter(|key| !key.is_empty())
            .collect();

        for key in &self.server.authorized_keys {
            if !is_valid_ed25519_public_key_hex(key) {
                return Err(ConfigError::Invalid(format!(
                    "server.authorized_keys contains invalid Ed25519 public key: {}",
                    key
                )));
            }
        }

        if self.server.authorized_keys.is_empty() && !self.server.insecure_allow_any_client {
            return Err(ConfigError::Invalid(
                "server.authorized_keys is empty. Configure an allowlist or set \
                 server.insecure_allow_any_client=true only for local development."
                    .to_string(),
            ));
        }

        Ok(())
    }
}

fn parse_bool(v: &str) -> bool {
    matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn is_valid_ed25519_public_key_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AgentConfig::default();
        assert_eq!(config.server.listen_port, 11411);
        assert_eq!(config.heartbeat.interval_secs, 30);
        assert_eq!(config.logging.level, "info");
        assert!(!config.server.insecure_allow_any_client);
    }

    #[test]
    fn test_parse_toml() {
        let toml_str = r#"
            [server]
            listen_port = 8080
            authorized_keys = ["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]

            [platform]
            api_url = "http://localhost:3000"
            api_key = "rob_test123"

            [webrtc]
            enabled = true
            robot_id = "robot-01"
            stun_timeout_secs = 5

            [heartbeat]
            interval_secs = 10
        "#;

        let mut config: AgentConfig = toml::from_str(toml_str).unwrap();
        config.validate_security().unwrap();
        assert_eq!(config.server.listen_port, 8080);
        assert_eq!(config.platform.api_key, Some("rob_test123".to_string()));
        assert!(config.webrtc.enabled);
        assert_eq!(config.webrtc.robot_id.as_deref(), Some("robot-01"));
        assert_eq!(config.webrtc.stun_timeout_secs, 5);
        assert_eq!(config.heartbeat.interval_secs, 10);
    }

    #[test]
    fn test_validate_security_rejects_empty_allowlist_by_default() {
        let mut config = AgentConfig::default();
        let err = config.validate_security().unwrap_err();
        assert!(err.to_string().contains("server.authorized_keys is empty"));
    }

    #[test]
    fn test_validate_security_allows_explicit_insecure_mode() {
        let mut config = AgentConfig::default();
        config.server.insecure_allow_any_client = true;
        config.validate_security().unwrap();
    }

    #[test]
    fn test_validate_security_normalizes_keys() {
        let mut config = AgentConfig::default();
        config.server.authorized_keys =
            vec![" 0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF ".to_string()];
        config.validate_security().unwrap();
        assert_eq!(
            config.server.authorized_keys,
            vec!["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]
        );
    }
}
