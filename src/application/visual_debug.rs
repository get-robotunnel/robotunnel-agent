use super::projection_plane::engine::{resolve_profile, ProjectionEngine, ProjectionFilters};
use super::projection_plane::stats::collect_topic_stats;
use super::projection_plane::{ProjectionMode, SessionStartRequest};
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use serde_json::{json, Value};
use std::sync::Arc;

pub(super) async fn handle_visual_debug_skill(
    req: CommandRequest,
    engine: Arc<ProjectionEngine>,
) -> CommandResponse {
    match req.action.as_str() {
        "list_profiles" => ok(req.id, engine.profiles()),
        "start" => handle_start(req, engine).await,
        "stop" => handle_stop(req, engine).await,
        "status" => handle_status(req, engine).await,
        "topic_stats" => handle_topic_stats(req).await,
        other => err(req.id, format!("unknown visual_debug action: {}", other)),
    }
}

async fn handle_start(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let mode = req
        .params
        .get("mode")
        .and_then(Value::as_str)
        .map(ProjectionMode::from_str)
        .unwrap_or(ProjectionMode::Foxglove);

    let transport_policy = req
        .params
        .get("transport_policy")
        .and_then(Value::as_str)
        .unwrap_or("tcp_only")
        .trim()
        .to_string();

    let requested_profile = req
        .params
        .get("profile")
        .and_then(Value::as_str)
        .unwrap_or("balanced");
    let (resolved_profile, profile_defaults) = resolve_profile(requested_profile);

    let desired_delay_ms = req
        .params
        .get("desired_delay_ms")
        .and_then(Value::as_u64)
        .unwrap_or(profile_defaults.desired_delay_ms)
        .clamp(0, 5000);

    let filters = ProjectionFilters {
        point_stride: read_u32_param(&req.params, "point_stride").or(profile_defaults.point_stride),
        voxel_leaf_m: read_f64_param(&req.params, "voxel_leaf_m").or(profile_defaults.voxel_leaf_m),
        image_scale: read_f64_param(&req.params, "image_scale").or(profile_defaults.image_scale),
        hz_limit: read_f64_param(&req.params, "hz_limit").or(profile_defaults.hz_limit),
    };

    let topics = req
        .params
        .get("topics")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let session_name = req
        .params
        .get("session_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    let vnc_port = read_u16_param(&req.params, "vnc_port");

    let session = engine
        .start_session(SessionStartRequest {
            session_name,
            mode,
            transport_policy,
            profile: resolved_profile,
            topics,
            desired_delay_ms,
            filters,
            vnc_port,
        })
        .await;

    ok(
        req.id,
        json!({
            "message": "visual debug projection session started",
            "session": session,
        }),
    )
}

async fn handle_stop(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let Some(session_id) = req.params.get("session_id").and_then(Value::as_str) else {
        return err(req.id, "missing required param: session_id".to_string());
    };
    match engine.stop_session(session_id.trim()).await {
        Some(session) => ok(
            req.id,
            json!({
                "message": "visual debug projection session stopped",
                "session": session,
            }),
        ),
        None => err(req.id, format!("session not found: {}", session_id.trim())),
    }
}

async fn handle_status(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let session_id = req
        .params
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let status = engine.status(session_id).await;
    ok(req.id, status)
}

async fn handle_topic_stats(req: CommandRequest) -> CommandResponse {
    let Some(topic) = req.params.get("topic").and_then(Value::as_str) else {
        return err(req.id, "missing required param: topic".to_string());
    };
    let window_sec = req
        .params
        .get("window_sec")
        .and_then(Value::as_u64)
        .unwrap_or(6);

    match collect_topic_stats(topic, window_sec).await {
        Ok(stats) => ok(
            req.id,
            json!({
                "source": "projection_stats_collector",
                "stats": stats,
            }),
        ),
        Err(msg) => err(req.id, msg),
    }
}

fn read_u32_param(params: &Value, key: &str) -> Option<u32> {
    params
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
}

fn read_u16_param(params: &Value, key: &str) -> Option<u16> {
    params
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u16::try_from(v).ok())
}

fn read_f64_param(params: &Value, key: &str) -> Option<f64> {
    params.get(key).and_then(Value::as_f64)
}

fn ok(id: String, data: Value) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Ok,
        data: Some(data),
        error: None,
    }
}

fn err(id: String, message: String) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Error,
        data: None,
        error: Some(message),
    }
}
