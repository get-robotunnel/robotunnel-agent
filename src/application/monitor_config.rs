use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorSettings {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_sample_interval_secs")]
    pub sample_interval_secs: u64,
    #[serde(default)]
    pub cpu_threshold_percent: Option<f64>,
    #[serde(default)]
    pub mem_threshold_percent: Option<f64>,
    #[serde(default = "default_notify")]
    pub notify: String,
    #[serde(default)]
    pub webhook_url: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
}

impl Default for MonitorSettings {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            sample_interval_secs: default_sample_interval_secs(),
            cpu_threshold_percent: None,
            mem_threshold_percent: None,
            notify: default_notify(),
            webhook_url: None,
            provider: None,
        }
    }
}

impl MonitorSettings {
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }

        let raw = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
        let config = toml::from_str(&raw).with_context(|| format!("parsing {:?}", path))?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {:?}", parent))?;
        }
        let encoded = toml::to_string_pretty(self).context("serializing monitor config")?;
        fs::write(&path, encoded).with_context(|| format!("writing {:?}", path))?;
        Ok(())
    }

    pub fn apply_overrides(&mut self, settings: &[String]) -> Result<Vec<String>> {
        if settings.is_empty() {
            bail!("provide at least one key=value setting");
        }

        let mut warnings = Vec::new();
        for item in settings {
            let (key, value) = parse_setting(item)?;
            self.apply_setting(key, value)?;
        }

        warnings.extend(self.validate_delivery());
        Ok(warnings)
    }

    pub fn apply_structured_settings(
        &mut self,
        settings: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<String>> {
        for (key, value) in settings {
            match key.as_str() {
                "enabled" => {
                    self.enabled = value
                        .as_bool()
                        .ok_or_else(|| anyhow!("enabled must be a boolean"))?;
                }
                "sample_interval_secs" => {
                    self.sample_interval_secs = value
                        .as_u64()
                        .ok_or_else(|| anyhow!("sample_interval_secs must be an integer"))?;
                }
                "cpu_threshold_percent" => {
                    self.cpu_threshold_percent = parse_json_threshold(value, key)?;
                }
                "mem_threshold_percent" => {
                    self.mem_threshold_percent = parse_json_threshold(value, key)?;
                }
                "notify" => {
                    let notify = value
                        .as_str()
                        .ok_or_else(|| anyhow!("notify must be a string"))?;
                    self.notify = normalize_notify(notify)?;
                }
                "webhook_url" => {
                    self.webhook_url = parse_json_optional_string(value, key)?;
                }
                "provider" => {
                    self.provider = parse_json_optional_string(value, key)?;
                }
                other => bail!("unknown monitor setting '{}'", other),
            }
        }

        Ok(self.validate_delivery())
    }

    fn apply_setting(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "enabled" => self.enabled = parse_bool(value)?,
            "sample_interval_secs" | "interval_secs" => {
                self.sample_interval_secs = parse_u64(value, key)?
            }
            "cpu_threshold" | "cpu_threshold_percent" => {
                self.cpu_threshold_percent = parse_optional_threshold(value, key)?
            }
            "mem_threshold" | "mem_threshold_percent" => {
                self.mem_threshold_percent = parse_optional_threshold(value, key)?
            }
            "notify" => self.notify = normalize_notify(value)?,
            "webhook_url" => {
                self.webhook_url = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            "provider" => {
                self.provider = if value.trim().is_empty() {
                    None
                } else {
                    Some(value.trim().to_string())
                };
            }
            other => bail!("unknown setting '{}'", other),
        }
        Ok(())
    }

    fn validate_delivery(&self) -> Vec<String> {
        if self.notify == "webhook" && self.webhook_url.is_none() {
            return vec![
                "notify requires webhook_url to be set before alerts can be delivered.".to_string(),
            ];
        }
        Vec::new()
    }
}

fn config_path() -> Result<PathBuf> {
    let base = std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|home| PathBuf::from(home).join(".config"))
        })
        .ok_or_else(|| anyhow!("unable to determine config directory"))?;

    Ok(base.join("robotunnel").join("monitor.toml"))
}

fn parse_setting(value: &str) -> Result<(&str, &str)> {
    let Some((key, raw)) = value.split_once('=') else {
        bail!("invalid setting '{}': expected key=value", value);
    };
    let key = key.trim();
    if key.is_empty() {
        bail!("invalid setting '{}': empty key", value);
    }
    Ok((key, raw.trim()))
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("invalid boolean '{}'", value),
    }
}

fn parse_u64(value: &str, field: &str) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{} must be a positive integer", field))
}

fn parse_optional_threshold(value: &str, field: &str) -> Result<Option<f64>> {
    let normalized = value.trim().to_lowercase();
    if normalized.is_empty() || matches!(normalized.as_str(), "auto" | "off" | "none") {
        return Ok(None);
    }

    let parsed = normalized
        .parse::<f64>()
        .with_context(|| format!("{} must be a percentage", field))?;
    validate_threshold(parsed, field)?;
    Ok(Some(parsed))
}

fn parse_json_threshold(value: &serde_json::Value, field: &str) -> Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }
    let parsed = value
        .as_f64()
        .ok_or_else(|| anyhow!("{} must be a number or null", field))?;
    validate_threshold(parsed, field)?;
    Ok(Some(parsed))
}

fn parse_json_optional_string(value: &serde_json::Value, field: &str) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let parsed = value
        .as_str()
        .ok_or_else(|| anyhow!("{} must be a string or null", field))?;
    if parsed.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed.trim().to_string()))
    }
}

fn validate_threshold(parsed: f64, field: &str) -> Result<()> {
    if !(0.0..=100.0).contains(&parsed) {
        bail!("{} must be between 0 and 100", field);
    }
    Ok(())
}

fn normalize_notify(value: &str) -> Result<String> {
    let normalized = value.trim().to_lowercase();
    if !matches!(
        normalized.as_str(),
        "log" | "webhook" | "platform" | "discord"
    ) {
        bail!("notify must be one of: log, webhook, platform, discord");
    }
    Ok(normalized)
}

fn default_enabled() -> bool {
    true
}

fn default_sample_interval_secs() -> u64 {
    30
}

fn default_notify() -> String {
    "log".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn structured_settings_allow_thresholds_and_nulls() {
        let mut settings = MonitorSettings::default();
        let payload = json!({
            "enabled": true,
            "cpu_threshold_percent": 85.0,
            "mem_threshold_percent": null,
            "notify": "platform",
            "provider": "openai"
        });

        let warnings = settings
            .apply_structured_settings(payload.as_object().unwrap())
            .unwrap();

        assert_eq!(settings.cpu_threshold_percent, Some(85.0));
        assert_eq!(settings.mem_threshold_percent, None);
        assert_eq!(settings.notify, "platform");
        assert_eq!(settings.provider.as_deref(), Some("openai"));
        assert_eq!(warnings.len(), 0);
    }

    #[test]
    fn invalid_threshold_is_rejected() {
        let mut settings = MonitorSettings::default();
        let payload = json!({
            "cpu_threshold_percent": 120.0
        });

        let err = settings
            .apply_structured_settings(payload.as_object().unwrap())
            .unwrap_err();

        assert!(err.to_string().contains("between 0 and 100"));
    }
}
