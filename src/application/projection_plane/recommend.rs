use super::engine::{resolve_profile, ProjectionMode};
use super::policy::TopicPolicyRule;
use rt_core::ros::ros2_shell_command;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

pub async fn build_recommendation(
    mode: ProjectionMode,
    transport_policy: &str,
    topics: &[String],
) -> Value {
    let normalized_transport_policy = normalize_transport_policy(transport_policy);
    let constrained_transport = is_constrained_transport(&normalized_transport_policy);

    let mut topic_analysis = Vec::new();
    let mut has_point_cloud = false;
    let mut has_raw_image = false;
    let mut has_compressed_image = false;
    let mut has_occupancy_grid = false;

    for topic in topics {
        let topic_type = read_topic_type(topic).await;
        if let Some(topic_type) = topic_type.as_deref() {
            match topic_type {
                "sensor_msgs/msg/PointCloud2" => has_point_cloud = true,
                "sensor_msgs/msg/Image" => has_raw_image = true,
                "sensor_msgs/msg/CompressedImage" => has_compressed_image = true,
                "nav_msgs/msg/OccupancyGrid" => has_occupancy_grid = true,
                _ => {}
            }
        }
        topic_analysis.push(json!({
            "topic": topic,
            "topic_type": topic_type,
        }));
    }

    let mut reasons = Vec::new();
    let profile = if mode == ProjectionMode::StatsOnly {
        reasons.push("mode=stats_only, prioritize low overhead metrics sampling".to_string());
        "stats_only".to_string()
    } else if constrained_transport && (has_raw_image || has_compressed_image) {
        reasons
            .push("constrained transport + image workload, prefer compressed resize".to_string());
        "compressed_resize".to_string()
    } else if constrained_transport && has_point_cloud {
        reasons.push(
            "constrained transport + point cloud workload, prefer lidar low bandwidth".to_string(),
        );
        "lidar_low_bw".to_string()
    } else if has_raw_image && !has_point_cloud {
        reasons.push("image-first workload detected, prefer compressed passthrough".to_string());
        "compressed_passthrough".to_string()
    } else {
        reasons.push("mixed or unknown workload, fall back to balanced profile".to_string());
        "balanced".to_string()
    };

    let (_, defaults) = resolve_profile(&profile);
    let mut topic_policy = BTreeMap::new();
    if has_occupancy_grid {
        reasons.push(
            "low-frequency occupancy grid detected, keep explicit low-rate throttle".to_string(),
        );
    }

    for item in &topic_analysis {
        let topic = item
            .get("topic")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let topic_type = item
            .get("topic_type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if topic.is_empty() || topic_type.is_empty() {
            continue;
        }

        match topic_type {
            "nav_msgs/msg/OccupancyGrid" => {
                topic_policy.insert(
                    topic.to_string(),
                    TopicPolicyRule {
                        transform: "throttle".to_string(),
                        max_hz: Some(0.2),
                        step: None,
                        voxel_size: None,
                        scale: None,
                        encode: None,
                        quality: None,
                    },
                );
            }
            "sensor_msgs/msg/PointCloud2" if constrained_transport => {
                topic_policy.insert(
                    topic.to_string(),
                    TopicPolicyRule {
                        transform: "voxel".to_string(),
                        max_hz: Some(5.0),
                        step: None,
                        voxel_size: Some(0.12),
                        scale: None,
                        encode: None,
                        quality: None,
                    },
                );
            }
            "sensor_msgs/msg/Image" if constrained_transport => {
                topic_policy.insert(
                    topic.to_string(),
                    TopicPolicyRule {
                        transform: "reencode_jpeg".to_string(),
                        max_hz: Some(6.0),
                        step: None,
                        voxel_size: None,
                        scale: Some(0.5),
                        encode: Some("jpeg".to_string()),
                        quality: Some(60),
                    },
                );
            }
            "sensor_msgs/msg/CompressedImage" if constrained_transport => {
                topic_policy.insert(
                    topic.to_string(),
                    TopicPolicyRule {
                        transform: "reencode_jpeg".to_string(),
                        max_hz: Some(6.0),
                        step: None,
                        voxel_size: None,
                        scale: Some(0.6),
                        encode: Some("jpeg".to_string()),
                        quality: Some(65),
                    },
                );
            }
            _ => {}
        }
    }

    let mut start_params = serde_json::Map::new();
    start_params.insert(
        "mode".to_string(),
        Value::String(mode_name(mode).to_string()),
    );
    start_params.insert(
        "transport_policy".to_string(),
        Value::String(normalized_transport_policy.clone()),
    );
    start_params.insert("profile".to_string(), Value::String(profile.clone()));
    if !topics.is_empty() {
        start_params.insert("topics".to_string(), json!(topics));
    }
    start_params.insert(
        "desired_delay_ms".to_string(),
        json!(defaults.desired_delay_ms),
    );
    start_params.insert(
        "tf_alignment_window_ms".to_string(),
        json!(defaults.tf_alignment_window_ms),
    );
    if !topic_policy.is_empty() {
        start_params.insert("topic_policy".to_string(), json!(topic_policy));
    }

    json!({
        "profile": profile,
        "start_params": start_params,
        "topic_policy": topic_policy,
        "topic_analysis": topic_analysis,
        "reasons": reasons,
        "constrained_transport": constrained_transport,
    })
}

fn mode_name(mode: ProjectionMode) -> &'static str {
    match mode {
        ProjectionMode::Foxglove => "foxglove",
        ProjectionMode::RvizVnc => "rviz_vnc",
        ProjectionMode::StatsOnly => "stats_only",
    }
}

fn normalize_transport_policy(input: &str) -> String {
    let value = input.trim().to_lowercase();
    if value.is_empty() {
        return "auto".to_string();
    }
    value
}

fn is_constrained_transport(policy: &str) -> bool {
    matches!(policy, "tcp_only" | "no_webrtc" | "tcp_preferred")
}

async fn read_topic_type(topic: &str) -> Option<String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return None;
    }
    let command = ros2_shell_command(&["topic", "type", topic]);
    let mut cmd = Command::new("bash");
    cmd.args(["-lc", &command]);

    let output = timeout(Duration::from_secs(4), cmd.output()).await.ok()?;
    let output = output.ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
