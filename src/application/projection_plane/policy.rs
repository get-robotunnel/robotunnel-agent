use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPolicyRule {
    #[serde(default = "default_transform")]
    pub transform: String,
    #[serde(default)]
    pub max_hz: Option<f64>,
    #[serde(default)]
    pub step: Option<u32>,
    #[serde(default)]
    pub voxel_size: Option<f64>,
    #[serde(default)]
    pub scale: Option<f64>,
    #[serde(default)]
    pub encode: Option<String>,
    #[serde(default)]
    pub quality: Option<u8>,
}

impl Default for TopicPolicyRule {
    fn default() -> Self {
        Self {
            transform: default_transform(),
            max_hz: None,
            step: None,
            voxel_size: None,
            scale: None,
            encode: None,
            quality: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicPolicySet {
    #[serde(default)]
    pub rules: BTreeMap<String, TopicPolicyRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedTopicPolicy {
    pub key: String,
    pub rule: TopicPolicyRule,
}

impl TopicPolicySet {
    pub fn builtin_defaults() -> Self {
        let mut rules = BTreeMap::new();
        rules.insert(
            "sensor_msgs/msg/PointCloud2".to_string(),
            TopicPolicyRule {
                transform: "voxel".to_string(),
                voxel_size: Some(0.05),
                max_hz: Some(6.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/Image".to_string(),
            TopicPolicyRule {
                transform: "reencode_jpeg".to_string(),
                scale: Some(0.5),
                encode: Some("jpeg".to_string()),
                quality: Some(60),
                max_hz: Some(8.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/CompressedImage".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(8.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/LaserScan".to_string(),
            TopicPolicyRule {
                transform: "stride".to_string(),
                step: Some(3),
                max_hz: Some(8.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "nav_msgs/msg/OccupancyGrid".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(0.1),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/Imu".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(10.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/JointState".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(20.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "geometry_msgs/msg/PoseWithCovarianceStamped".to_string(),
            TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "nav_msgs/msg/Path".to_string(),
            TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "sensor_msgs/msg/CameraInfo".to_string(),
            TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "rosgraph_msgs/msg/Log".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(50.0),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "tf2_msgs/msg/TFMessage".to_string(),
            TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
        );
        rules.insert(
            "*".to_string(),
            TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(20.0),
                ..TopicPolicyRule::default()
            },
        );
        Self { rules }
    }

    pub fn from_params(value: &Value) -> Result<Self, String> {
        let Some(map) = value.as_object() else {
            return Err("topic_policy must be an object".to_string());
        };
        let mut rules = BTreeMap::new();
        for (key, raw_rule) in map {
            let mut rule: TopicPolicyRule = serde_json::from_value(raw_rule.clone())
                .map_err(|err| format!("invalid topic_policy rule '{}': {}", key, err))?;
            rule.normalize();
            rules.insert(key.trim().to_string(), rule);
        }
        Ok(Self { rules })
    }

    pub fn merged_with(&self, overrides: &Self) -> Self {
        let mut rules = self.rules.clone();
        for (key, rule) in &overrides.rules {
            rules.insert(key.clone(), rule.clone());
        }
        Self { rules }
    }

    pub fn resolve(&self, topic: &str, topic_type: Option<&str>) -> ResolvedTopicPolicy {
        let topic = topic.trim();
        let topic_type = topic_type.map(str::trim).filter(|v| !v.is_empty());

        if matches!(topic_type, Some("tf2_msgs/msg/TFMessage")) {
            return ResolvedTopicPolicy {
                key: "tf2_msgs/msg/TFMessage".to_string(),
                rule: TopicPolicyRule {
                    transform: "passthrough".to_string(),
                    ..TopicPolicyRule::default()
                },
            };
        }

        if let Some(rule) = self
            .rules
            .get(topic)
            .cloned()
            .or_else(|| topic_type.and_then(|tt| self.rules.get(tt).cloned()))
            .or_else(|| self.rules.get("*").cloned())
        {
            return ResolvedTopicPolicy {
                key: if self.rules.contains_key(topic) {
                    topic.to_string()
                } else if let Some(tt) = topic_type {
                    if self.rules.contains_key(tt) {
                        tt.to_string()
                    } else {
                        "*".to_string()
                    }
                } else {
                    "*".to_string()
                },
                rule,
            };
        }

        ResolvedTopicPolicy {
            key: "builtin_fallback".to_string(),
            rule: TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
        }
    }
}

pub fn profile_topic_policy_overrides(profile: &str) -> Option<TopicPolicySet> {
    let normalized = profile.trim().to_lowercase();
    if normalized.is_empty() {
        return None;
    }

    let mut rules = BTreeMap::new();
    match normalized.as_str() {
        "compressed_passthrough" => {
            rules.insert(
                "sensor_msgs/msg/CompressedImage".to_string(),
                TopicPolicyRule {
                    transform: "throttle".to_string(),
                    max_hz: Some(8.0),
                    ..TopicPolicyRule::default()
                },
            );
        }
        "compressed_resize" => {
            rules.insert(
                "sensor_msgs/msg/CompressedImage".to_string(),
                TopicPolicyRule {
                    transform: "reencode_jpeg".to_string(),
                    scale: Some(0.5),
                    encode: Some("jpeg".to_string()),
                    quality: Some(60),
                    max_hz: Some(6.0),
                    ..TopicPolicyRule::default()
                },
            );
        }
        _ => return None,
    }

    Some(TopicPolicySet { rules })
}

impl TopicPolicyRule {
    pub fn normalize(&mut self) {
        self.transform = normalize_transform(&self.transform);
        self.max_hz = self.max_hz.filter(|v| *v > 0.0);
        self.step = self.step.filter(|v| *v > 0);
        self.voxel_size = self.voxel_size.filter(|v| *v > 0.0);
        self.scale = self.scale.map(|v| v.clamp(0.05, 1.0));
        self.quality = self.quality.map(|v| v.clamp(1, 100));
        self.encode = self
            .encode
            .as_ref()
            .map(|v| v.trim().to_lowercase())
            .filter(|v| !v.is_empty());
    }
}

fn default_transform() -> String {
    "passthrough".to_string()
}

fn normalize_transform(raw: &str) -> String {
    match raw.trim().to_lowercase().as_str() {
        "throttle" => "throttle".to_string(),
        "stride" => "stride".to_string(),
        "voxel" => "voxel".to_string(),
        "resize" => "resize".to_string(),
        "reencode_jpeg" | "jpeg" | "reencode" => "reencode_jpeg".to_string(),
        _ => "passthrough".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tf_is_forced_to_passthrough() {
        let set = TopicPolicySet::builtin_defaults();
        let resolved = set.resolve("/tf", Some("tf2_msgs/msg/TFMessage"));
        assert_eq!(resolved.rule.transform, "passthrough");
    }

    #[test]
    fn resolve_prefers_topic_over_type_over_star() {
        let mut set = TopicPolicySet::builtin_defaults();
        set.rules.insert(
            "/scan".to_string(),
            TopicPolicyRule {
                transform: "stride".to_string(),
                step: Some(7),
                ..TopicPolicyRule::default()
            },
        );
        let resolved = set.resolve("/scan", Some("sensor_msgs/msg/LaserScan"));
        assert_eq!(resolved.key, "/scan");
        assert_eq!(resolved.rule.step, Some(7));
    }

    #[test]
    fn profile_override_for_compressed_resize_exists() {
        let overrides = profile_topic_policy_overrides("compressed_resize")
            .expect("compressed_resize profile override");
        let resolved = overrides.resolve(
            "/camera/image/compressed",
            Some("sensor_msgs/msg/CompressedImage"),
        );
        assert_eq!(resolved.rule.transform, "reencode_jpeg");
        assert_eq!(resolved.rule.scale, Some(0.5));
        assert_eq!(resolved.rule.encode.as_deref(), Some("jpeg"));
    }
}
