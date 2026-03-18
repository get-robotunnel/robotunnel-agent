use super::projection_plane::policy::{TopicPolicyRule, TopicPolicySet};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualDebugSettings {
    #[serde(default)]
    pub ros2: VisualDebugRos2Settings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualDebugRos2Settings {
    #[serde(default = "default_topic_policy")]
    pub topic_policy: BTreeMap<String, TopicPolicyRule>,
}

impl Default for VisualDebugSettings {
    fn default() -> Self {
        Self {
            ros2: VisualDebugRos2Settings::default(),
        }
    }
}

impl Default for VisualDebugRos2Settings {
    fn default() -> Self {
        Self {
            topic_policy: default_topic_policy(),
        }
    }
}

impl VisualDebugSettings {
    pub fn load() -> Result<Self> {
        let path = config_path()?;
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
        let mut cfg: Self = toml::from_str(&raw).with_context(|| format!("parsing {:?}", path))?;
        cfg.normalize();
        Ok(cfg)
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {:?}", parent))?;
        }
        let encoded = toml::to_string_pretty(self).context("serializing visual debug config")?;
        fs::write(&path, encoded).with_context(|| format!("writing {:?}", path))?;
        Ok(())
    }

    pub fn apply_structured_settings(
        &mut self,
        settings: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        for (key, value) in settings {
            match key.as_str() {
                "ros2" => {
                    let obj = value
                        .as_object()
                        .ok_or_else(|| anyhow!("ros2 must be an object"))?;
                    self.apply_ros2_settings(obj)?;
                }
                other => bail!("unknown visual_debug setting '{}'", other),
            }
        }
        self.normalize();
        Ok(())
    }

    pub fn topic_policy_set(&self) -> TopicPolicySet {
        TopicPolicySet {
            rules: self.ros2.topic_policy.clone(),
        }
    }

    fn apply_ros2_settings(
        &mut self,
        settings: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        for (key, value) in settings {
            match key.as_str() {
                "topic_policy" => {
                    let mut rules = BTreeMap::new();
                    let Some(policy_obj) = value.as_object() else {
                        bail!("ros2.topic_policy must be an object");
                    };
                    for (pattern, raw_rule) in policy_obj {
                        let mut rule: TopicPolicyRule = serde_json::from_value(raw_rule.clone())
                            .map_err(|err| {
                                anyhow!("invalid ros2.topic_policy rule '{}': {}", pattern, err)
                            })?;
                        rule.normalize();
                        rules.insert(pattern.trim().to_string(), rule);
                    }
                    self.ros2.topic_policy = rules;
                }
                other => bail!("unknown visual_debug.ros2 setting '{}'", other),
            }
        }
        Ok(())
    }

    fn normalize(&mut self) {
        for rule in self.ros2.topic_policy.values_mut() {
            rule.normalize();
        }
    }
}

fn default_topic_policy() -> BTreeMap<String, TopicPolicyRule> {
    TopicPolicySet::builtin_defaults().rules
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

    Ok(base.join("robotunnel").join("visual_debug.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn structured_settings_update_topic_policy() {
        let mut cfg = VisualDebugSettings::default();
        let payload = json!({
            "ros2": {
                "topic_policy": {
                    "sensor_msgs/msg/Imu": {
                        "transform": "throttle",
                        "max_hz": 12.0
                    }
                }
            }
        });
        cfg.apply_structured_settings(payload.as_object().unwrap())
            .expect("apply settings");
        let imu = cfg
            .ros2
            .topic_policy
            .get("sensor_msgs/msg/Imu")
            .expect("imu rule");
        assert_eq!(imu.transform, "throttle");
        assert_eq!(imu.max_hz, Some(12.0));
    }
}
