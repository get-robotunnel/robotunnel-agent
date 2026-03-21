use super::engine::{ProjectionFilters, ProjectionMode};
use super::policy::{TopicPolicyRule, TopicPolicySet};
use serde::{Deserialize, Serialize};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicProjectionRoute {
    pub source_topic: String,
    pub source_topic_type: Option<String>,
    pub projected_topic: String,
    pub projected_topic_type: Option<String>,
    pub policy_key: Option<String>,
    pub policy_transform: Option<String>,
    pub status: String,
    pub ownership: String,
    pub launch_attempted: bool,
    pub launch_command: Option<String>,
    pub applied_filters: Vec<String>,
    pub unsupported_filters: Vec<String>,
    pub message: Option<String>,
    pub checked_at_unix: u64,
}

pub struct TransformProcessHandle {
    child: Child,
}

const EMBEDDED_PROJECTION_WORKER: &str = include_str!("../../../scripts/projection_worker.py");

impl TransformProcessHandle {
    pub async fn stop(&mut self) {
        if self.child.id().is_none() {
            return;
        }

        let _ = self.child.start_kill();
        let _ = timeout(Duration::from_secs(2), self.child.wait()).await;
    }
}

pub async fn ensure_session_transforms(
    session_id: &str,
    mode: ProjectionMode,
    topics: &[String],
    filters: &ProjectionFilters,
    topic_policy: &TopicPolicySet,
) -> (
    Vec<TopicProjectionRoute>,
    Vec<TransformProcessHandle>,
    Vec<String>,
) {
    if mode != ProjectionMode::Foxglove || topics.is_empty() {
        return (Vec::new(), Vec::new(), Vec::new());
    }

    let mut routes = Vec::with_capacity(topics.len());
    let mut handles = Vec::new();
    let mut notes = Vec::new();
    let namespace = session_namespace(session_id);
    let worker_script = locate_projection_worker_script();

    for source_topic in topics {
        let projected_topic = build_projected_topic(&namespace, source_topic);
        let topic_type = read_topic_type(source_topic).await;
        let resolved_policy = topic_policy.resolve(source_topic, topic_type.as_deref());
        let effective_filters =
            compose_topic_filters(filters, &resolved_policy.rule, topic_type.as_deref());
        let launch = build_projection_launch(
            source_topic,
            &projected_topic,
            topic_type.as_deref(),
            &effective_filters,
            &resolved_policy.rule,
            worker_script.as_deref(),
        );
        let (mut applied_filters, mut unsupported_filters) = classify_filter_support(
            topic_type.as_deref(),
            &effective_filters,
            &resolved_policy.rule,
            &launch.supported,
        );
        let command = launch.command;

        let (route, handle) = spawn_projection_route(
            source_topic,
            topic_type.clone(),
            projected_topic,
            launch.projected_topic_type.clone(),
            Some(resolved_policy.key.clone()),
            Some(resolved_policy.rule.transform.clone()),
            &command,
            &mut applied_filters,
            &mut unsupported_filters,
        )
        .await;

        if route.status == "running" {
            notes.push(format!(
                "projection route {} -> {} policy={} src_type={} dst_type={}",
                route.source_topic,
                route.projected_topic,
                route
                    .policy_transform
                    .clone()
                    .unwrap_or_else(|| "passthrough".to_string()),
                route
                    .source_topic_type
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                route
                    .projected_topic_type
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            ));
            if !route.unsupported_filters.is_empty() {
                notes.push(format!(
                    "projection route {} pending support: {}",
                    route.source_topic,
                    route.unsupported_filters.join(", ")
                ));
            }
        } else {
            notes.push(format!(
                "projection route {} unavailable: {}",
                route.source_topic,
                route
                    .message
                    .as_deref()
                    .unwrap_or("failed to launch projection process")
            ));
        }

        if let Some(handle) = handle {
            handles.push(handle);
        }
        routes.push(route);
    }

    (routes, handles, notes)
}

async fn spawn_projection_route(
    source_topic: &str,
    source_topic_type: Option<String>,
    projected_topic: String,
    projected_topic_type: Option<String>,
    policy_key: Option<String>,
    policy_transform: Option<String>,
    command: &str,
    applied_filters: &mut Vec<String>,
    unsupported_filters: &mut Vec<String>,
) -> (TopicProjectionRoute, Option<TransformProcessHandle>) {
    let mut cmd = Command::new("sh");
    cmd.args(["-lc", command]);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return (
                TopicProjectionRoute {
                    source_topic: source_topic.to_string(),
                    source_topic_type,
                    projected_topic,
                    projected_topic_type,
                    policy_key,
                    policy_transform,
                    status: "error".to_string(),
                    ownership: "session_managed".to_string(),
                    launch_attempted: true,
                    launch_command: Some(command.to_string()),
                    applied_filters: applied_filters.clone(),
                    unsupported_filters: unsupported_filters.clone(),
                    message: Some(format!("spawn failed: {}", err)),
                    checked_at_unix: unix_now(),
                },
                None,
            );
        }
    };

    sleep(Duration::from_millis(300)).await;
    match child.try_wait() {
        Ok(Some(exit_status)) => (
            TopicProjectionRoute {
                source_topic: source_topic.to_string(),
                source_topic_type,
                projected_topic,
                projected_topic_type,
                policy_key,
                policy_transform,
                status: "error".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: true,
                launch_command: Some(command.to_string()),
                applied_filters: applied_filters.clone(),
                unsupported_filters: unsupported_filters.clone(),
                message: Some(format!(
                    "projection process exited early with {}",
                    exit_status
                )),
                checked_at_unix: unix_now(),
            },
            None,
        ),
        Ok(None) => (
            TopicProjectionRoute {
                source_topic: source_topic.to_string(),
                source_topic_type,
                projected_topic,
                projected_topic_type,
                policy_key,
                policy_transform,
                status: "running".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: true,
                launch_command: Some(command.to_string()),
                applied_filters: applied_filters.clone(),
                unsupported_filters: unsupported_filters.clone(),
                message: Some("projection process running".to_string()),
                checked_at_unix: unix_now(),
            },
            Some(TransformProcessHandle { child }),
        ),
        Err(err) => (
            TopicProjectionRoute {
                source_topic: source_topic.to_string(),
                source_topic_type,
                projected_topic,
                projected_topic_type,
                policy_key,
                policy_transform,
                status: "error".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: true,
                launch_command: Some(command.to_string()),
                applied_filters: applied_filters.clone(),
                unsupported_filters: unsupported_filters.clone(),
                message: Some(format!("projection process check failed: {}", err)),
                checked_at_unix: unix_now(),
            },
            None,
        ),
    }
}

#[derive(Default)]
struct SupportedFilters {
    hz_limit: bool,
    point_stride: bool,
    voxel_leaf_m: bool,
    image_scale: bool,
    laser_stride: bool,
    image_encode: bool,
}

struct ProjectionLaunch {
    command: String,
    projected_topic_type: Option<String>,
    supported: SupportedFilters,
}

fn build_projection_launch(
    source_topic: &str,
    projected_topic: &str,
    topic_type: Option<&str>,
    filters: &ProjectionFilters,
    policy_rule: &TopicPolicyRule,
    worker_script: Option<&Path>,
) -> ProjectionLaunch {
    let source = shell_quote(source_topic);
    let projected = shell_quote(projected_topic);
    let transform = policy_rule.transform.as_str();

    if matches!(topic_type, Some("tf2_msgs/msg/TFMessage")) {
        return ProjectionLaunch {
            command: format!("exec ros2 run topic_tools relay {} {}", source, projected),
            projected_topic_type: topic_type.map(ToString::to_string),
            supported: SupportedFilters::default(),
        };
    }

    let wants_worker = transform_requires_worker(transform)
        || filters.point_stride.filter(|v| *v > 1).is_some()
        || filters.voxel_leaf_m.filter(|v| *v > 0.0).is_some()
        || filters
            .image_scale
            .filter(|v| *v > 0.0 && *v < 0.999)
            .is_some();

    if wants_worker {
        if let Some(script) = worker_script {
            if can_worker_process(topic_type, transform) {
                let command = build_projection_worker_command(
                    script,
                    source_topic,
                    projected_topic,
                    filters,
                    policy_rule,
                );
                return ProjectionLaunch {
                    command,
                    projected_topic_type: projected_topic_type_for(topic_type, policy_rule),
                    supported: SupportedFilters {
                        hz_limit: true,
                        point_stride: true,
                        voxel_leaf_m: true,
                        image_scale: true,
                        laser_stride: true,
                        image_encode: true,
                    },
                };
            }
        }
    }

    if let Some(hz) = filters.hz_limit.filter(|v| *v > 0.0) {
        return ProjectionLaunch {
            command: format!(
                "exec ros2 run topic_tools throttle messages {} {:.3} {}",
                source, hz, projected
            ),
            projected_topic_type: topic_type.map(ToString::to_string),
            supported: SupportedFilters {
                hz_limit: true,
                ..SupportedFilters::default()
            },
        };
    }

    ProjectionLaunch {
        command: format!("exec ros2 run topic_tools relay {} {}", source, projected),
        projected_topic_type: topic_type.map(ToString::to_string),
        supported: SupportedFilters::default(),
    }
}

fn can_worker_process(topic_type: Option<&str>, transform: &str) -> bool {
    match topic_type.unwrap_or("") {
        "sensor_msgs/msg/PointCloud2" => {
            matches!(transform, "voxel" | "stride" | "throttle" | "passthrough")
        }
        "sensor_msgs/msg/Image" => matches!(
            transform,
            "resize" | "reencode_jpeg" | "throttle" | "passthrough"
        ),
        "sensor_msgs/msg/CompressedImage" => matches!(
            transform,
            "resize" | "reencode_jpeg" | "throttle" | "passthrough"
        ),
        "sensor_msgs/msg/LaserScan" => matches!(transform, "stride" | "throttle" | "passthrough"),
        _ => matches!(transform, "throttle" | "passthrough"),
    }
}

fn projected_topic_type_for(
    topic_type: Option<&str>,
    policy_rule: &TopicPolicyRule,
) -> Option<String> {
    if matches!(topic_type, Some("sensor_msgs/msg/Image"))
        && policy_rule
            .encode
            .as_deref()
            .map(|v| v.eq_ignore_ascii_case("jpeg"))
            .unwrap_or(false)
        && policy_rule.transform == "reencode_jpeg"
    {
        return Some("sensor_msgs/msg/CompressedImage".to_string());
    }
    topic_type.map(ToString::to_string)
}

fn transform_requires_worker(transform: &str) -> bool {
    matches!(transform, "voxel" | "stride" | "resize" | "reencode_jpeg")
}

fn compose_topic_filters(
    base: &ProjectionFilters,
    policy: &TopicPolicyRule,
    topic_type: Option<&str>,
) -> ProjectionFilters {
    if matches!(topic_type, Some("tf2_msgs/msg/TFMessage")) {
        return ProjectionFilters::default();
    }

    let mut out = base.clone();

    if out.hz_limit.is_none() {
        out.hz_limit = policy.max_hz;
    }

    if out.point_stride.is_none() {
        out.point_stride = policy.step;
    }

    if out.voxel_leaf_m.is_none() {
        out.voxel_leaf_m = policy.voxel_size;
    }

    if out.image_scale.is_none() {
        out.image_scale = policy.scale;
    }

    if policy.transform == "passthrough" {
        out.point_stride = None;
        out.voxel_leaf_m = None;
        out.image_scale = None;
        return out;
    }

    let is_pc = matches!(topic_type, Some("sensor_msgs/msg/PointCloud2"));
    let is_scan = matches!(topic_type, Some("sensor_msgs/msg/LaserScan"));
    let is_image = matches!(
        topic_type,
        Some("sensor_msgs/msg/Image") | Some("sensor_msgs/msg/CompressedImage")
    );

    if !(is_pc || is_scan) {
        out.point_stride = None;
        out.voxel_leaf_m = None;
    }
    if !is_image {
        out.image_scale = None;
    }
    if is_scan {
        out.voxel_leaf_m = None;
    }

    out
}

fn build_projection_worker_command(
    script: &Path,
    source_topic: &str,
    projected_topic: &str,
    filters: &ProjectionFilters,
    policy_rule: &TopicPolicyRule,
) -> String {
    let mut args = vec![
        shell_quote("python3"),
        shell_quote(script.to_string_lossy().as_ref()),
        shell_quote("--source-topic"),
        shell_quote(source_topic),
        shell_quote("--projected-topic"),
        shell_quote(projected_topic),
    ];

    if let Some(hz) = filters.hz_limit.filter(|v| *v > 0.0) {
        args.push(shell_quote("--hz-limit"));
        args.push(shell_quote(&format!("{:.6}", hz)));
    }
    if let Some(stride) = filters.point_stride.filter(|v| *v > 1) {
        args.push(shell_quote("--point-stride"));
        args.push(shell_quote(&stride.to_string()));
    }
    if let Some(voxel) = filters.voxel_leaf_m.filter(|v| *v > 0.0) {
        args.push(shell_quote("--voxel-leaf-m"));
        args.push(shell_quote(&format!("{:.6}", voxel)));
    }
    if let Some(scale) = filters.image_scale.filter(|v| *v > 0.0 && *v < 0.999) {
        args.push(shell_quote("--image-scale"));
        args.push(shell_quote(&format!("{:.6}", scale)));
    }
    if let Some(encode) = policy_rule
        .encode
        .as_ref()
        .map(|v| v.trim().to_lowercase())
        .filter(|v| !v.is_empty())
    {
        args.push(shell_quote("--encode"));
        args.push(shell_quote(&encode));
    }
    if let Some(quality) = policy_rule.quality {
        args.push(shell_quote("--quality"));
        args.push(shell_quote(&quality.to_string()));
    }

    format!("exec {}", args.join(" "))
}

fn classify_filter_support(
    topic_type: Option<&str>,
    filters: &ProjectionFilters,
    policy_rule: &TopicPolicyRule,
    support: &SupportedFilters,
) -> (Vec<String>, Vec<String>) {
    let mut applied = Vec::new();
    let mut unsupported = Vec::new();

    applied.push(format!("transform={}", policy_rule.transform));

    if let Some(hz) = filters.hz_limit.filter(|v| *v > 0.0) {
        if support.hz_limit {
            applied.push(format!("hz_limit={:.3}", hz));
        } else {
            unsupported.push(format!("hz_limit={:.3}", hz));
        }
    }
    if let Some(stride) = filters.point_stride.filter(|v| *v > 1) {
        let is_scan = matches!(topic_type, Some("sensor_msgs/msg/LaserScan"));
        if (is_scan && support.laser_stride) || (!is_scan && support.point_stride) {
            applied.push(format!("point_stride={}", stride));
        } else {
            unsupported.push(format!("point_stride={}", stride));
        }
    }
    if let Some(voxel) = filters.voxel_leaf_m.filter(|v| *v > 0.0) {
        if support.voxel_leaf_m {
            applied.push(format!("voxel_leaf_m={:.3}", voxel));
        } else {
            unsupported.push(format!("voxel_leaf_m={:.3}", voxel));
        }
    }
    if let Some(scale) = filters.image_scale.filter(|v| *v > 0.0 && *v < 0.999) {
        if support.image_scale {
            applied.push(format!("image_scale={:.3}", scale));
        } else {
            unsupported.push(format!("image_scale={:.3}", scale));
        }
    }

    if let Some(encode) = policy_rule
        .encode
        .as_ref()
        .map(|v| v.trim().to_lowercase())
        .filter(|v| !v.is_empty())
    {
        if support.image_encode {
            applied.push(format!("encode={}", encode));
        } else {
            unsupported.push(format!("encode={}", encode));
        }
    }
    (applied, unsupported)
}

async fn read_topic_type(topic: &str) -> Option<String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return None;
    }

    let mut cmd = Command::new("ros2");
    cmd.args(["topic", "type", topic]);
    let output = timeout(Duration::from_secs(3), cmd.output())
        .await
        .ok()?
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let value = raw.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn locate_projection_worker_script() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("RT_PROJECTION_WORKER_PATH") {
        let candidate = PathBuf::from(raw.trim());
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    let cwd_candidate = PathBuf::from("scripts/projection_worker.py");
    if cwd_candidate.is_file() {
        return Some(cwd_candidate);
    }

    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.parent().map(Path::to_path_buf).unwrap_or_default();
        p.push("scripts");
        p.push("projection_worker.py");
        if p.is_file() {
            return Some(p);
        }
    }

    materialize_embedded_projection_worker().ok()
}

fn materialize_embedded_projection_worker() -> Result<PathBuf, String> {
    let mut target = std::env::temp_dir();
    target.push("rt_projection_worker.py");

    match fs::read_to_string(&target) {
        Ok(existing) if existing == EMBEDDED_PROJECTION_WORKER => return Ok(target),
        Ok(_) => {}
        Err(_) => {}
    }

    fs::write(&target, EMBEDDED_PROJECTION_WORKER).map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(0o755);
        let _ = fs::set_permissions(&target, perms);
    }
    Ok(target)
}

fn build_projected_topic(namespace: &str, source_topic: &str) -> String {
    let slug = sanitize_topic(source_topic);
    format!("/rt/debug/{}/{}", namespace, slug)
}

fn sanitize_topic(topic: &str) -> String {
    let trimmed = topic.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return "topic".to_string();
    }

    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    let collapsed = out
        .split('_')
        .filter(|seg| !seg.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if collapsed.is_empty() {
        "topic".to_string()
    } else {
        collapsed
    }
}

fn session_namespace(session_id: &str) -> String {
    let raw = session_id.trim();
    if raw.is_empty() {
        return "session".to_string();
    }
    let compact = raw
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_lowercase();
    if compact.is_empty() {
        return "session".to_string();
    }
    compact.chars().take(10).collect()
}

fn shell_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\"'\"'");
    format!("'{}'", escaped)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_projected_topic_uses_session_namespace() {
        let topic = build_projected_topic("abc123", "/lidar/points");
        assert_eq!(topic, "/rt/debug/abc123/lidar_points");
    }

    #[test]
    fn compose_topic_filters_respects_passthrough() {
        let base = ProjectionFilters {
            point_stride: Some(4),
            voxel_leaf_m: Some(0.1),
            image_scale: Some(0.6),
            hz_limit: Some(5.0),
        };
        let rule = TopicPolicyRule {
            transform: "passthrough".to_string(),
            ..TopicPolicyRule::default()
        };
        let out = compose_topic_filters(&base, &rule, Some("sensor_msgs/msg/PointCloud2"));
        assert_eq!(out.hz_limit, Some(5.0));
        assert!(out.point_stride.is_none());
        assert!(out.voxel_leaf_m.is_none());
        assert!(out.image_scale.is_none());
    }

    #[test]
    fn compose_topic_filters_keeps_image_scale_for_compressed_image() {
        let base = ProjectionFilters::default();
        let rule = TopicPolicyRule {
            transform: "reencode_jpeg".to_string(),
            scale: Some(0.4),
            ..TopicPolicyRule::default()
        };
        let out = compose_topic_filters(&base, &rule, Some("sensor_msgs/msg/CompressedImage"));
        assert_eq!(out.image_scale, Some(0.4));
    }

    #[test]
    fn build_projection_launch_passthrough_with_hz_uses_throttle() {
        let filters = ProjectionFilters {
            hz_limit: Some(2.0),
            ..ProjectionFilters::default()
        };
        let launch = build_projection_launch(
            "/camera/compressed",
            "/rt/debug/s1/camera_compressed",
            Some("sensor_msgs/msg/CompressedImage"),
            &filters,
            &TopicPolicyRule {
                transform: "passthrough".to_string(),
                max_hz: Some(2.0),
                ..TopicPolicyRule::default()
            },
            None,
        );
        assert!(launch.command.contains("topic_tools throttle messages"));
        assert!(launch.supported.hz_limit);
    }

    #[test]
    fn build_projection_launch_uses_worker_for_compressed_image_resize() {
        let filters = ProjectionFilters {
            image_scale: Some(0.5),
            hz_limit: Some(6.0),
            ..ProjectionFilters::default()
        };
        let launch = build_projection_launch(
            "/camera/image/compressed",
            "/rt/debug/s1/camera_image_compressed",
            Some("sensor_msgs/msg/CompressedImage"),
            &filters,
            &TopicPolicyRule {
                transform: "reencode_jpeg".to_string(),
                encode: Some("jpeg".to_string()),
                quality: Some(60),
                ..TopicPolicyRule::default()
            },
            Some(std::path::Path::new("/tmp/worker.py")),
        );
        assert!(launch.command.contains("python3"));
        assert!(launch.command.contains("--image-scale"));
        assert!(launch.command.contains("--encode"));
        assert!(launch.supported.image_scale);
    }

    #[test]
    fn build_projection_launch_prefers_throttle_when_worker_unavailable() {
        let filters = ProjectionFilters {
            hz_limit: Some(1.5),
            ..ProjectionFilters::default()
        };
        let launch = build_projection_launch(
            "/scan",
            "/rt/debug/s1/scan",
            Some("sensor_msgs/msg/LaserScan"),
            &filters,
            &TopicPolicyRule {
                transform: "throttle".to_string(),
                max_hz: Some(1.5),
                ..TopicPolicyRule::default()
            },
            None,
        );
        assert!(launch.command.contains("topic_tools throttle messages"));
        assert!(launch.supported.hz_limit);
    }

    #[test]
    fn build_projection_launch_uses_worker_for_transform_filters() {
        let filters = ProjectionFilters {
            point_stride: Some(8),
            image_scale: Some(0.5),
            ..ProjectionFilters::default()
        };
        let launch = build_projection_launch(
            "/camera/image",
            "/rt/debug/s1/camera_image",
            Some("sensor_msgs/msg/Image"),
            &filters,
            &TopicPolicyRule {
                transform: "resize".to_string(),
                ..TopicPolicyRule::default()
            },
            Some(std::path::Path::new("/tmp/worker.py")),
        );
        assert!(launch.command.contains("python3"));
        assert!(launch.command.contains("--point-stride"));
        assert!(launch.command.contains("--image-scale"));
        assert!(launch.supported.point_stride);
        assert!(launch.supported.image_scale);
    }

    #[test]
    fn build_projection_launch_keeps_delay_only_passthrough_on_relay_path() {
        let filters = ProjectionFilters::default();
        let launch = build_projection_launch(
            "/scan",
            "/rt/debug/s1/scan",
            Some("sensor_msgs/msg/LaserScan"),
            &filters,
            &TopicPolicyRule {
                transform: "passthrough".to_string(),
                ..TopicPolicyRule::default()
            },
            Some(std::path::Path::new("/tmp/worker.py")),
        );
        assert!(launch.command.contains("topic_tools relay"));
    }

    #[test]
    fn embedded_worker_can_be_materialized() {
        let path = materialize_embedded_projection_worker().expect("worker path");
        assert!(path.is_file());
        let raw = std::fs::read_to_string(path).expect("worker script");
        assert!(raw.contains("class ProjectionWorker"));
    }
}
