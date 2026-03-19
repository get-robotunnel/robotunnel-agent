use super::projection_plane::engine::{
    resolve_profile, ProjectionEngine, ProjectionFilters, TopicRuntimeView,
};
use super::projection_plane::policy::{profile_topic_policy_overrides, TopicPolicySet};
use super::projection_plane::recommend::build_recommendation;
use super::projection_plane::stats::collect_topic_stats;
use super::projection_plane::{ProjectionMode, SessionStartRequest};
use super::visual_debug_config::VisualDebugSettings;
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
        "recommend" => handle_recommend(req, engine).await,
        "topic_stats" => handle_topic_stats(req, engine).await,
        "stream_pull" => handle_stream_pull(req, engine).await,
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
        .unwrap_or("auto")
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
    let tf_alignment_window_ms = req
        .params
        .get("tf_alignment_window_ms")
        .and_then(Value::as_u64)
        .unwrap_or(profile_defaults.tf_alignment_window_ms)
        .clamp(0, 5000);

    let filters = ProjectionFilters {
        point_stride: read_u32_param(&req.params, "point_stride").or(profile_defaults.point_stride),
        voxel_leaf_m: read_f64_param(&req.params, "voxel_leaf_m").or(profile_defaults.voxel_leaf_m),
        image_scale: read_f64_param(&req.params, "image_scale").or(profile_defaults.image_scale),
        hz_limit: read_f64_param(&req.params, "hz_limit").or(profile_defaults.hz_limit),
    };

    let topics = parse_topics(&req.params);

    let session_name = req
        .params
        .get("session_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    let vnc_port = read_u16_param(&req.params, "vnc_port");
    let topic_policy = match resolve_topic_policy(&req.params, &resolved_profile) {
        Ok(policy) => policy,
        Err(message) => return err(req.id, message),
    };

    let session = engine
        .start_session(SessionStartRequest {
            session_name,
            mode,
            transport_policy,
            profile: resolved_profile,
            topics,
            desired_delay_ms,
            tf_alignment_window_ms,
            filters,
            topic_policy,
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

fn resolve_topic_policy(params: &Value, resolved_profile: &str) -> Result<TopicPolicySet, String> {
    let mut base = match VisualDebugSettings::load() {
        Ok(cfg) => cfg.topic_policy_set(),
        Err(_) => TopicPolicySet::builtin_defaults(),
    };

    if let Some(profile_overrides) = profile_topic_policy_overrides(resolved_profile) {
        base = base.merged_with(&profile_overrides);
    }

    if let Some(raw_overrides) = params.get("topic_policy") {
        let overrides = TopicPolicySet::from_params(raw_overrides)?;
        base = base.merged_with(&overrides);
    }
    Ok(base)
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

async fn handle_recommend(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let requested_mode = req
        .params
        .get("mode")
        .and_then(Value::as_str)
        .map(ProjectionMode::from_str);
    let mut mode = requested_mode.unwrap_or(ProjectionMode::Foxglove);
    let transport_policy = req
        .params
        .get("transport_policy")
        .and_then(Value::as_str)
        .unwrap_or("auto")
        .trim()
        .to_string();
    let mut topics = parse_topics(&req.params);
    let session_id = req
        .params
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());

    if topics.is_empty() {
        if let Some(session_id) = session_id {
            if let Some(snapshot) = engine.session_snapshot(session_id).await {
                topics = snapshot.topics;
                if requested_mode.is_none() {
                    mode = snapshot.mode;
                }
            }
        }
    }

    let recommendation = build_recommendation(mode, &transport_policy, &topics).await;
    ok(
        req.id,
        json!({
            "source": "projection_recommendation",
            "recommendation": recommendation,
        }),
    )
}

async fn handle_topic_stats(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let Some(topic) = req.params.get("topic").and_then(Value::as_str) else {
        return err(req.id, "missing required param: topic".to_string());
    };
    let window_sec = req
        .params
        .get("window_sec")
        .and_then(Value::as_u64)
        .unwrap_or(6);
    let session_id = req
        .params
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let runtime_view = engine
        .topic_runtime_view(topic, session_id, window_sec)
        .await;
    let runtime_payload = runtime_view
        .as_ref()
        .map(|view| build_runtime_projection_payload(view, window_sec));

    if let Some(view) = runtime_view.as_ref() {
        if view.estimated.sample_count > 0 {
            return ok(
                req.id,
                json!({
                    "source": "projection_runtime_sampler",
                    "runtime_primary": true,
                    "collector_sparse": view.estimated.collector_sparse_hint,
                    "runtime_projection": runtime_payload,
                    "stats": build_runtime_stats_view(view, window_sec),
                }),
            );
        }
    }

    match collect_topic_stats(topic, window_sec).await {
        Ok(stats) => {
            let collector_sparse = stats.average_hz.is_none()
                && stats.average_bw.is_none()
                && stats.average_delay_sec.is_none();
            let source = if runtime_payload.is_some() {
                "projection_hybrid_sampler"
            } else {
                "projection_stats_collector"
            };
            ok(
                req.id,
                json!({
                    "source": source,
                    "runtime_primary": false,
                    "collector_sparse": collector_sparse,
                    "runtime_projection": runtime_payload,
                    "stats": stats,
                }),
            )
        }
        Err(msg) => {
            if let Some(view) = runtime_view.as_ref() {
                return ok(
                    req.id,
                    json!({
                        "source": "projection_runtime_sampler",
                        "runtime_primary": true,
                        "collector_sparse": true,
                        "collector_error": msg,
                        "runtime_projection": runtime_payload,
                        "stats": build_runtime_stats_view(view, window_sec),
                    }),
                );
            }
            err(req.id, msg)
        }
    }
}

async fn handle_stream_pull(req: CommandRequest, engine: Arc<ProjectionEngine>) -> CommandResponse {
    let Some(session_id) = req.params.get("session_id").and_then(Value::as_str) else {
        return err(req.id, "missing required param: session_id".to_string());
    };
    let Some(topic) = req.params.get("topic").and_then(Value::as_str) else {
        return err(req.id, "missing required param: topic".to_string());
    };

    let since_seq = req.params.get("since_seq").and_then(Value::as_u64);
    let limit = req
        .params
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(8)
        .clamp(1, 64) as usize;

    let Some(payload) = engine
        .stream_pull(session_id.trim(), topic.trim(), since_seq, limit)
        .await
    else {
        return err(req.id, "session/topic buffer not found".to_string());
    };

    ok(
        req.id,
        json!({
            "source": "projection_stream_buffer",
            "stream": payload,
        }),
    )
}

fn read_u32_param(params: &Value, key: &str) -> Option<u32> {
    params
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
}

fn parse_topics(params: &Value) -> Vec<String> {
    params
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
        .unwrap_or_default()
}

fn read_u16_param(params: &Value, key: &str) -> Option<u16> {
    params
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| u16::try_from(v).ok())
}

fn build_runtime_projection_payload(view: &TopicRuntimeView, window_sec: u64) -> Value {
    json!({
        "source": "projection_runtime_sampler",
        "topic_resolution": {
            "requested_topic": view.requested_topic,
            "source_topic": view.source_topic,
            "projected_topic": view.projected_topic,
            "route_status": view.route_status,
        },
        "stats": view.stats,
        "estimated": {
            "sample_count": view.estimated.sample_count,
            "sample_window_sec": view.estimated.sample_window_sec,
            "average_hz": view.estimated.average_hz,
            "average_bw": view.estimated.average_bw,
            "average_bw_bps": view.estimated.average_bw_bps,
            "average_delay_sec": view.estimated.average_delay_sec,
            "collector_sparse_hint": view.estimated.collector_sparse_hint,
            "window_sec": window_sec,
        },
    })
}

fn build_runtime_stats_view(view: &TopicRuntimeView, window_sec: u64) -> Value {
    json!({
        "topic": view.source_topic,
        "requested_topic": view.requested_topic,
        "window_sec": window_sec,
        "average_hz": view.estimated.average_hz,
        "average_bw": view.estimated.average_bw,
        "average_delay_sec": view.estimated.average_delay_sec,
        "raw_hz": format!(
            "runtime_estimate sample_count={} window_sec={:.3}",
            view.estimated.sample_count, view.estimated.sample_window_sec
        ),
        "raw_bw": format!(
            "runtime_estimate average_bw_bps={}",
            view.estimated
                .average_bw_bps
                .map(|v| format!("{:.4}", v))
                .unwrap_or_else(|| "n/a".to_string())
        ),
        "raw_delay": format!(
            "runtime_estimate desired_delay_sec={}",
            view.estimated
                .average_delay_sec
                .map(|v| format!("{:.4}", v))
                .unwrap_or_else(|| "n/a".to_string())
        ),
    })
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
