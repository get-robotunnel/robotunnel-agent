use super::engine::ProjectionFilters;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicProjectionStats {
    pub topic: String,
    pub status: String,
    pub topic_type: Option<String>,
    pub captures: u64,
    pub failures: u64,
    pub last_capture_unix: Option<u64>,
    pub last_failure_unix: Option<u64>,
    pub last_success_age: Option<u64>,
    pub last_sample_status: Option<String>,
    pub last_error: Option<String>,
    pub input_bytes: Option<u64>,
    pub output_bytes: Option<u64>,
    pub input_points: Option<u64>,
    pub output_points: Option<u64>,
    pub input_width_px: Option<u32>,
    pub input_height_px: Option<u32>,
    pub output_width_px: Option<u32>,
    pub output_height_px: Option<u32>,
    pub applied_filters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectionRuntimeState {
    pub data_plane_enabled: bool,
    pub worker_state: String,
    pub total_captures: u64,
    pub total_failures: u64,
    pub last_tick_unix: Option<u64>,
    pub topic_stats: Vec<TopicProjectionStats>,
}

impl ProjectionRuntimeState {
    pub fn new(topics: &[String]) -> Self {
        let mut topic_stats = Vec::with_capacity(topics.len());
        for topic in topics {
            topic_stats.push(TopicProjectionStats {
                topic: topic.clone(),
                status: "pending".to_string(),
                ..Default::default()
            });
        }
        Self {
            data_plane_enabled: !topics.is_empty(),
            worker_state: if topics.is_empty() {
                "idle".to_string()
            } else {
                "starting".to_string()
            },
            total_captures: 0,
            total_failures: 0,
            last_tick_unix: None,
            topic_stats,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicProjectionSample {
    pub topic_type: Option<String>,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub input_points: Option<u64>,
    pub output_points: Option<u64>,
    pub input_width_px: Option<u32>,
    pub input_height_px: Option<u32>,
    pub output_width_px: Option<u32>,
    pub output_height_px: Option<u32>,
    pub applied_filters: Vec<String>,
    pub input_preview: Option<String>,
    pub output_preview: Option<String>,
}

pub async fn sample_topic_projection(
    topic: &str,
    filters: &ProjectionFilters,
    preferred_topic_type: Option<&str>,
) -> Result<TopicProjectionSample, String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return Err("empty topic".to_string());
    }

    let topic_type = resolve_topic_type(topic, preferred_topic_type).await;
    let sample = read_topic_sample(topic, topic_type.as_deref()).await?;
    let raw_bytes = sample.len() as u64;

    if let Some(tt) = topic_type.as_deref() {
        if tt.contains("sensor_msgs/msg/Image") {
            return Ok(project_image_sample(
                &sample, raw_bytes, filters, topic_type,
            ));
        }
        if tt.contains("sensor_msgs/msg/PointCloud2") {
            return Ok(project_point_cloud_sample(
                &sample, raw_bytes, filters, topic_type,
            ));
        }
    }

    Ok(TopicProjectionSample {
        topic_type,
        input_bytes: raw_bytes,
        output_bytes: raw_bytes,
        input_points: None,
        output_points: None,
        input_width_px: None,
        input_height_px: None,
        output_width_px: None,
        output_height_px: None,
        applied_filters: Vec::new(),
        input_preview: preview_text(&sample, 220),
        output_preview: preview_text(&sample, 220),
    })
}

async fn resolve_topic_type(topic: &str, preferred_topic_type: Option<&str>) -> Option<String> {
    if let Some(preferred) = sanitize_topic_type(preferred_topic_type) {
        return Some(preferred.to_string());
    }

    for _ in 0..3 {
        if let Ok(kind) = read_topic_type(topic).await {
            return Some(kind);
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    None
}

async fn read_topic_sample(topic: &str, topic_type: Option<&str>) -> Result<String, String> {
    let mut last_err: Option<String> = None;
    for attempt in build_topic_echo_attempts(topic, topic_type) {
        match run_topic_echo(
            topic,
            attempt.topic_type,
            attempt.transient_local,
            attempt.timeout_sec,
        )
        .await
        {
            Ok(out) if !out.trim().is_empty() => return Ok(out),
            Ok(_) => {
                last_err = Some(format!(
                    "empty topic sample (transient_local={})",
                    attempt.transient_local
                ))
            }
            Err(err) => {
                last_err = Some(format!(
                    "{} (transient_local={})",
                    err, attempt.transient_local
                ))
            }
        }
    }

    Err(last_err.unwrap_or_else(|| "failed to sample topic".to_string()))
}

#[derive(Clone, Copy)]
struct TopicEchoAttempt<'a> {
    topic_type: Option<&'a str>,
    transient_local: bool,
    timeout_sec: u64,
}

fn build_topic_echo_attempts<'a>(
    topic: &str,
    topic_type: Option<&'a str>,
) -> Vec<TopicEchoAttempt<'a>> {
    let mut attempts = Vec::new();
    let low_frequency = is_low_frequency_topic(topic, topic_type);
    if let Some(topic_type) = sanitize_topic_type(topic_type) {
        if low_frequency {
            attempts.push(TopicEchoAttempt {
                topic_type: Some(topic_type),
                transient_local: true,
                timeout_sec: 6,
            });
            attempts.push(TopicEchoAttempt {
                topic_type: Some(topic_type),
                transient_local: false,
                timeout_sec: 4,
            });
        } else {
            attempts.push(TopicEchoAttempt {
                topic_type: Some(topic_type),
                transient_local: false,
                timeout_sec: 4,
            });
            attempts.push(TopicEchoAttempt {
                topic_type: Some(topic_type),
                transient_local: true,
                timeout_sec: 3,
            });
        }
    }

    if low_frequency {
        attempts.push(TopicEchoAttempt {
            topic_type: None,
            transient_local: true,
            timeout_sec: 4,
        });
        attempts.push(TopicEchoAttempt {
            topic_type: None,
            transient_local: false,
            timeout_sec: 3,
        });
    } else {
        attempts.push(TopicEchoAttempt {
            topic_type: None,
            transient_local: false,
            timeout_sec: 4,
        });
        attempts.push(TopicEchoAttempt {
            topic_type: None,
            transient_local: true,
            timeout_sec: 3,
        });
    }

    attempts
}

fn is_low_frequency_topic(topic: &str, topic_type: Option<&str>) -> bool {
    let normalized_topic = topic.trim().to_ascii_lowercase();
    if normalized_topic == "/map"
        || normalized_topic.ends_with("/map")
        || normalized_topic.ends_with("/map_metadata")
        || normalized_topic == "/tf_static"
    {
        return true;
    }

    if let Some(topic_type) = sanitize_topic_type(topic_type) {
        return matches!(
            topic_type,
            "nav_msgs/msg/OccupancyGrid" | "nav_msgs/msg/MapMetaData"
        );
    }
    false
}

fn project_image_sample(
    sample: &str,
    input_bytes: u64,
    filters: &ProjectionFilters,
    topic_type: Option<String>,
) -> TopicProjectionSample {
    let width = parse_u64_line(sample, "width:")
        .and_then(|v| u32::try_from(v).ok())
        .filter(|v| *v > 0);
    let height = parse_u64_line(sample, "height:")
        .and_then(|v| u32::try_from(v).ok())
        .filter(|v| *v > 0);

    let scale = filters.image_scale.unwrap_or(1.0).clamp(0.05, 1.0);
    let mut applied_filters = Vec::new();
    if scale < 0.999 {
        applied_filters.push(format!("image_scale={:.3}", scale));
    }

    let (output_width, output_height) = match (width, height) {
        (Some(w), Some(h)) => {
            let ow = ((w as f64) * scale).round().max(1.0) as u32;
            let oh = ((h as f64) * scale).round().max(1.0) as u32;
            (Some(ow), Some(oh))
        }
        _ => (None, None),
    };

    let output_bytes = ((input_bytes as f64) * scale * scale).round() as u64;

    TopicProjectionSample {
        topic_type,
        input_bytes,
        output_bytes,
        input_points: None,
        output_points: None,
        input_width_px: width,
        input_height_px: height,
        output_width_px: output_width,
        output_height_px: output_height,
        applied_filters,
        input_preview: preview_text(sample, 220),
        output_preview: Some(format!(
            "scaled_image {}x{} -> {}x{} (scale={:.3})",
            width.unwrap_or(0),
            height.unwrap_or(0),
            output_width.unwrap_or(0),
            output_height.unwrap_or(0),
            scale
        )),
    }
}

fn project_point_cloud_sample(
    sample: &str,
    input_bytes: u64,
    filters: &ProjectionFilters,
    topic_type: Option<String>,
) -> TopicProjectionSample {
    let width = parse_u64_line(sample, "width:");
    let height = parse_u64_line(sample, "height:").unwrap_or(1).max(1);
    let point_step = parse_u64_line(sample, "point_step:");
    let row_step = parse_u64_line(sample, "row_step:");

    let input_points =
        width
            .map(|w| w.saturating_mul(height))
            .or_else(|| match (point_step, row_step) {
                (Some(ps), Some(rs)) if ps > 0 => Some((rs / ps).saturating_mul(height)),
                _ => None,
            });

    let stride = filters.point_stride.unwrap_or(1).max(1) as u64;
    let output_points = input_points.map(|points| ceil_div(points, stride));

    let mut applied_filters = Vec::new();
    if stride > 1 {
        applied_filters.push(format!("point_stride={}", stride));
    }
    if let Some(voxel) = filters.voxel_leaf_m {
        if voxel > 0.0 {
            applied_filters.push(format!("voxel_leaf_m={:.3}", voxel));
        }
    }

    let output_bytes = if stride > 1 {
        ceil_div(input_bytes, stride)
    } else {
        input_bytes
    };

    TopicProjectionSample {
        topic_type,
        input_bytes,
        output_bytes,
        input_points,
        output_points,
        input_width_px: None,
        input_height_px: None,
        output_width_px: None,
        output_height_px: None,
        applied_filters,
        input_preview: preview_text(sample, 220),
        output_preview: Some(format!(
            "point_cloud points={} -> {} stride={} voxel_leaf_m={}",
            input_points.unwrap_or(0),
            output_points.unwrap_or(0),
            stride,
            filters
                .voxel_leaf_m
                .map(|v| format!("{:.3}", v))
                .unwrap_or_else(|| "n/a".to_string())
        )),
    }
}

async fn read_topic_type(topic: &str) -> Result<String, String> {
    if let Ok(raw) = run_ros2_cmd(
        &["topic", "type", "--no-daemon", "--spin-time", "1", topic],
        5,
    )
    .await
    {
        if let Some(v) = parse_topic_type_output(&raw) {
            return Ok(v);
        }
    }

    let raw = run_ros2_cmd(
        &["topic", "list", "-t", "--no-daemon", "--spin-time", "1"],
        5,
    )
    .await?;
    if let Some(v) = parse_topic_type_from_list(&raw, topic) {
        return Ok(v);
    }

    Err("empty topic type".to_string())
}

async fn run_topic_echo(
    topic: &str,
    topic_type: Option<&str>,
    transient_local: bool,
    timeout_sec: u64,
) -> Result<String, String> {
    let timeout_arg = timeout_sec.clamp(2, 30).to_string();
    let mut args = vec!["topic", "echo", "--no-daemon", "--spin-time", "1", topic];
    if let Some(topic_type) = sanitize_topic_type(topic_type) {
        args.push(topic_type);
    }
    args.push("--once");
    args.push("--timeout");
    args.push(timeout_arg.as_str());
    if transient_local {
        args.push("--qos-durability");
        args.push("transient_local");
    }

    let command = build_ros2_shell_command(&args);
    let mut cmd = Command::new("bash");
    cmd.args(["-lc", &command]);

    run_command(cmd, timeout_sec.saturating_add(2)).await
}

async fn run_ros2_cmd(args: &[&str], timeout_sec: u64) -> Result<String, String> {
    let command = build_ros2_shell_command(args);
    let mut cmd = Command::new("bash");
    cmd.args(["-lc", &command]);
    run_command(cmd, timeout_sec).await
}

fn build_ros2_shell_command(args: &[&str]) -> String {
    let joined = args
        .iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ");

    format!(
        "if command -v ros2 >/dev/null 2>&1; then \
ros2 {joined}; \
elif [ -f /opt/ros/jazzy/setup.bash ]; then \
source /opt/ros/jazzy/setup.bash >/dev/null 2>&1 && ros2 {joined}; \
elif [ -x /opt/ros/jazzy/bin/ros2 ]; then \
/opt/ros/jazzy/bin/ros2 {joined}; \
elif [ -f /opt/ros/humble/setup.bash ]; then \
source /opt/ros/humble/setup.bash >/dev/null 2>&1 && ros2 {joined}; \
elif [ -x /opt/ros/humble/bin/ros2 ]; then \
/opt/ros/humble/bin/ros2 {joined}; \
else ros2 {joined}; fi"
    )
}

fn shell_escape(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

async fn run_command(mut cmd: Command, timeout_sec: u64) -> Result<String, String> {
    let duration = Duration::from_secs(timeout_sec.clamp(2, 30));
    cmd.kill_on_drop(true);
    let output = timeout(duration, cmd.output())
        .await
        .map_err(|_| format!("timeout after {}s", duration.as_secs()))?
        .map_err(|err| err.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = stderr.trim();
        if msg.is_empty() {
            return Err(format!("ros2 command exited with {}", output.status));
        }
        return Err(msg.to_string());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn sanitize_topic_type(topic_type: Option<&str>) -> Option<&str> {
    topic_type.map(str::trim).filter(|v| !v.is_empty())
}

fn parse_topic_type_output(raw: &str) -> Option<String> {
    raw.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(ToString::to_string)
}

fn parse_topic_type_from_list(raw: &str, topic: &str) -> Option<String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return None;
    }

    for line in raw.lines() {
        let line = line.trim();
        if !line.starts_with(topic) {
            continue;
        }
        let open = line.find('[')?;
        let close = line[open + 1..].find(']')?;
        let candidate = line[open + 1..open + 1 + close].trim();
        if !candidate.is_empty() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn parse_u64_line(raw: &str, key: &str) -> Option<u64> {
    raw.lines().find_map(|line| {
        let trimmed = line.trim();
        let rest = trimmed.strip_prefix(key)?.trim();
        let token = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(',');
        token.parse::<u64>().ok()
    })
}

fn ceil_div(a: u64, b: u64) -> u64 {
    if b == 0 {
        return a;
    }
    (a + b - 1) / b
}

fn preview_text(raw: &str, max_chars: usize) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed
        .lines()
        .take(24)
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ");
    let max_chars = max_chars.max(20);
    if normalized.chars().count() <= max_chars {
        return Some(normalized);
    }
    let mut out = String::new();
    for ch in normalized.chars().take(max_chars) {
        out.push(ch);
    }
    out.push_str("...");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_u64_line_extracts_values() {
        let raw = "width: 640\nheight: 480\npoint_step: 16\n";
        assert_eq!(parse_u64_line(raw, "width:"), Some(640));
        assert_eq!(parse_u64_line(raw, "height:"), Some(480));
        assert_eq!(parse_u64_line(raw, "point_step:"), Some(16));
    }

    #[test]
    fn image_projection_applies_scale() {
        let filters = ProjectionFilters {
            image_scale: Some(0.5),
            ..ProjectionFilters::default()
        };
        let raw = "height: 720\nwidth: 1280\nencoding: rgb8\n";
        let sample = project_image_sample(
            raw,
            1000,
            &filters,
            Some("sensor_msgs/msg/Image".to_string()),
        );
        assert_eq!(sample.input_width_px, Some(1280));
        assert_eq!(sample.output_width_px, Some(640));
        assert!(sample.output_bytes < sample.input_bytes);
    }

    #[test]
    fn point_cloud_projection_applies_stride() {
        let filters = ProjectionFilters {
            point_stride: Some(10),
            ..ProjectionFilters::default()
        };
        let raw = "height: 1\nwidth: 1000\npoint_step: 16\nrow_step: 16000\n";
        let sample = project_point_cloud_sample(
            raw,
            16000,
            &filters,
            Some("sensor_msgs/msg/PointCloud2".to_string()),
        );
        assert_eq!(sample.input_points, Some(1000));
        assert_eq!(sample.output_points, Some(100));
        assert!(sample.output_bytes < sample.input_bytes);
    }

    #[test]
    fn low_frequency_sampling_prefers_transient_local_first() {
        let attempts = build_topic_echo_attempts("/map", Some("nav_msgs/msg/OccupancyGrid"));
        assert_eq!(attempts.len(), 4);
        assert!(attempts[0].transient_local);
        assert_eq!(attempts[0].timeout_sec, 6);
        assert!(!attempts[1].transient_local);
    }

    #[test]
    fn high_frequency_sampling_prefers_volatile_first() {
        let attempts = build_topic_echo_attempts("/scan", Some("sensor_msgs/msg/LaserScan"));
        assert_eq!(attempts.len(), 4);
        assert!(!attempts[0].transient_local);
        assert_eq!(attempts[0].timeout_sec, 4);
        assert!(attempts[1].transient_local);
    }

    #[test]
    fn low_frequency_topic_detection_accepts_tf_static_name() {
        assert!(is_low_frequency_topic("/tf_static", None));
        assert!(!is_low_frequency_topic(
            "/scan",
            Some("sensor_msgs/msg/LaserScan")
        ));
    }
}
