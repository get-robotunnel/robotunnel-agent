use super::adapters::{build_endpoints, ProjectionEndpoints};
use super::bridge::{ensure_mode_bridges, BridgeProcessHandle, BridgeServiceState};
use super::buffers::{SessionLocalBuffer, StreamMessage};
use super::policy::TopicPolicySet;
use super::profiles::{builtin_profiles, ProjectionDefaults};
use super::runtime::{
    sample_topic_projection, ProjectionRuntimeState, TopicProjectionSample, TopicProjectionStats,
};
use super::transforms::{ensure_session_transforms, TopicProjectionRoute, TransformProcessHandle};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionMode {
    Foxglove,
    RvizVnc,
    StatsOnly,
}

impl ProjectionMode {
    pub fn from_str(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "rviz_vnc" | "rviz" => Self::RvizVnc,
            "stats_only" | "stats" => Self::StatsOnly,
            _ => Self::Foxglove,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectionFilters {
    pub point_stride: Option<u32>,
    pub voxel_leaf_m: Option<f64>,
    pub image_scale: Option<f64>,
    pub hz_limit: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStartRequest {
    pub session_name: Option<String>,
    pub mode: ProjectionMode,
    pub transport_policy: String,
    pub profile: String,
    pub topics: Vec<String>,
    pub desired_delay_ms: u64,
    pub tf_alignment_window_ms: u64,
    pub filters: ProjectionFilters,
    pub topic_policy: TopicPolicySet,
    pub vnc_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionSession {
    pub session_id: String,
    pub session_name: Option<String>,
    pub status: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub mode: ProjectionMode,
    pub transport_policy: String,
    pub profile: String,
    pub topics: Vec<String>,
    pub tf_alignment_window_ms: u64,
    pub filters: ProjectionFilters,
    pub topic_policy: TopicPolicySet,
    pub session_buffer: SessionLocalBuffer,
    pub endpoints: ProjectionEndpoints,
    pub bridge_services: Vec<BridgeServiceState>,
    pub topic_routes: Vec<TopicProjectionRoute>,
    pub notes: Vec<String>,
    pub runtime: ProjectionRuntimeState,
}

struct RunningProjectionSession {
    session: ProjectionSession,
    stop_tx: watch::Sender<bool>,
    worker: Option<JoinHandle<()>>,
    bridge_processes: Vec<BridgeProcessHandle>,
    transform_processes: Vec<TransformProcessHandle>,
}

pub struct ProjectionEngine {
    sessions: Arc<Mutex<HashMap<String, RunningProjectionSession>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEstimatedStats {
    pub sample_count: usize,
    pub sample_window_sec: f64,
    pub average_hz: Option<f64>,
    pub average_bw: Option<String>,
    pub average_bw_bps: Option<f64>,
    pub average_delay_sec: Option<f64>,
    pub collector_sparse_hint: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRuntimeView {
    pub requested_topic: String,
    pub source_topic: String,
    pub projected_topic: Option<String>,
    pub route_status: String,
    pub stats: TopicProjectionStats,
    pub estimated: RuntimeEstimatedStats,
}

#[derive(Debug, Clone)]
struct TopicAliasResolution {
    requested_topic: String,
    source_topic: String,
    projected_topic: Option<String>,
    route_status: String,
}

impl ProjectionEngine {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn profiles(&self) -> serde_json::Value {
        serde_json::json!({
            "profiles": builtin_profiles(),
        })
    }

    pub async fn start_session(&self, req: SessionStartRequest) -> ProjectionSession {
        let now = unix_now();
        let session_id = Uuid::new_v4().to_string();
        let endpoints = build_endpoints(req.mode, req.vnc_port);
        let (bridge_services, bridge_processes, bridge_notes) =
            ensure_mode_bridges(req.mode, &endpoints).await;
        let topics = normalize_topics(req.topics);
        let (topic_routes, transform_processes, transform_notes) = ensure_session_transforms(
            &session_id,
            req.mode,
            &topics,
            &req.filters,
            &req.topic_policy,
        )
        .await;
        let runtime = ProjectionRuntimeState::new(&topics);
        let mut session_buffer = SessionLocalBuffer::new(req.desired_delay_ms);
        for topic in &topics {
            session_buffer.ensure_topic(topic);
        }

        let mut notes = vec![
            "projection configured session-locally; production subscribers remain unchanged"
                .to_string(),
            "data-plane sampler reads source topics on robot side and maintains session-local projection stats"
                .to_string(),
        ];
        notes.extend(bridge_notes);
        notes.extend(transform_notes);

        let session = ProjectionSession {
            session_id: session_id.clone(),
            session_name: req.session_name,
            status: "running".to_string(),
            created_at_unix: now,
            updated_at_unix: now,
            mode: req.mode,
            transport_policy: req.transport_policy,
            profile: req.profile,
            topics: topics.clone(),
            tf_alignment_window_ms: req.tf_alignment_window_ms,
            filters: req.filters.clone(),
            topic_policy: req.topic_policy.clone(),
            session_buffer,
            endpoints,
            bridge_services,
            topic_routes,
            notes,
            runtime,
        };

        let (stop_tx, stop_rx) = watch::channel(false);
        let entry = RunningProjectionSession {
            session: session.clone(),
            stop_tx,
            worker: None,
            bridge_processes,
            transform_processes,
        };

        {
            let mut guard = self.sessions.lock().await;
            guard.insert(session_id.clone(), entry);
        }

        if !topics.is_empty() {
            let worker = tokio::spawn(run_projection_worker(
                self.sessions.clone(),
                session_id.clone(),
                stop_rx,
                topics,
                req.filters,
            ));
            let mut guard = self.sessions.lock().await;
            if let Some(entry) = guard.get_mut(&session_id) {
                entry.worker = Some(worker);
                entry.session.runtime.worker_state = "running".to_string();
                entry.session.updated_at_unix = unix_now();
                return entry.session.clone();
            }
        }

        session
    }

    pub async fn stop_session(&self, session_id: &str) -> Option<ProjectionSession> {
        let mut removed = {
            let mut guard = self.sessions.lock().await;
            guard.remove(session_id)?
        };

        let _ = removed.stop_tx.send(true);
        if let Some(worker) = removed.worker.take() {
            worker.abort();
        }
        for process in &mut removed.bridge_processes {
            process.stop().await;
        }
        for process in &mut removed.transform_processes {
            process.stop().await;
        }

        removed.session.status = "stopped".to_string();
        removed.session.updated_at_unix = unix_now();
        removed.session.runtime.worker_state = "stopped".to_string();
        Some(removed.session)
    }

    pub async fn status(&self, session_id: Option<&str>) -> serde_json::Value {
        let mut guard = self.sessions.lock().await;
        let now = unix_now();
        if let Some(id) = session_id {
            if let Some(entry) = guard.get_mut(id) {
                refresh_success_ages(&mut entry.session.runtime, now);
                return serde_json::json!({"session": session_status_view(&entry.session)});
            }
            return serde_json::json!({"session": serde_json::Value::Null});
        }

        let sessions = guard
            .values_mut()
            .map(|entry| {
                refresh_success_ages(&mut entry.session.runtime, now);
                session_status_view(&entry.session)
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "count": sessions.len(),
            "sessions": sessions,
        })
    }

    pub async fn session_snapshot(&self, session_id: &str) -> Option<ProjectionSession> {
        let session_id = session_id.trim();
        if session_id.is_empty() {
            return None;
        }
        let mut guard = self.sessions.lock().await;
        let now = unix_now();
        let entry = guard.get_mut(session_id)?;
        refresh_success_ages(&mut entry.session.runtime, now);
        Some(entry.session.clone())
    }

    pub async fn topic_runtime_view(
        &self,
        topic: &str,
        session_id: Option<&str>,
        window_sec: u64,
    ) -> Option<TopicRuntimeView> {
        let topic = topic.trim();
        if topic.is_empty() {
            return None;
        }

        let mut guard = self.sessions.lock().await;
        let now_unix = unix_now();
        let now_ms = unix_now_ms();
        let window_sec = window_sec.clamp(3, 60);

        if let Some(session_id) = session_id {
            let entry = guard.get_mut(session_id)?;
            refresh_success_ages(&mut entry.session.runtime, now_unix);
            return build_runtime_view_for_session(&entry.session, topic, window_sec, now_ms);
        }

        let mut best: Option<(u64, TopicRuntimeView)> = None;
        for entry in guard.values_mut() {
            refresh_success_ages(&mut entry.session.runtime, now_unix);
            let Some(view) =
                build_runtime_view_for_session(&entry.session, topic, window_sec, now_ms)
            else {
                continue;
            };
            let updated = entry.session.updated_at_unix;
            let replace = best.as_ref().map(|(ts, _)| updated >= *ts).unwrap_or(true);
            if replace {
                best = Some((updated, view));
            }
        }
        best.map(|(_, view)| view)
    }

    pub async fn stream_pull(
        &self,
        session_id: &str,
        topic: &str,
        since_seq: Option<u64>,
        limit: usize,
    ) -> Option<serde_json::Value> {
        let session_id = session_id.trim();
        let topic = topic.trim();
        if session_id.is_empty() || topic.is_empty() {
            return None;
        }

        let guard = self.sessions.lock().await;
        let entry = guard.get(session_id)?;
        let resolved = resolve_topic_alias(&entry.session, topic)?;
        let all_messages = entry
            .session
            .session_buffer
            .pull_messages_since(&resolved.source_topic, since_seq)?;
        let stream_meta = entry
            .session
            .session_buffer
            .topics
            .get(&resolved.source_topic)?;
        let now_ms = unix_now_ms();
        let (messages, delayed_messages, alignment_skipped) = select_aligned_messages(
            &all_messages,
            now_ms,
            entry.session.session_buffer.desired_delay_ms,
            entry.session.tf_alignment_window_ms,
            limit.max(1),
        );
        let requested_topic = resolved.requested_topic.clone();
        let source_topic = resolved.source_topic.clone();
        let projected_topic = resolved.projected_topic.clone();
        let since_value = since_seq.unwrap_or(0);
        let next_since_seq = messages.last().map(|msg| msg.seq).unwrap_or(since_value);
        let target_capture_unix_ms =
            now_ms.saturating_sub(entry.session.session_buffer.desired_delay_ms);
        Some(serde_json::json!({
            "session_id": session_id,
            "topic": source_topic.clone(),
            "requested_topic": requested_topic,
            "display_topic": source_topic.clone(),
            "source_topic": source_topic,
            "transport_topic": projected_topic.clone(),
            "projected_topic": projected_topic,
            "since_seq": since_seq,
            "next_since_seq": next_since_seq,
            "returned": messages.len(),
            "last_seq": stream_meta.last_seq,
            "dropped_messages": stream_meta.dropped_messages,
            "delayed_messages": delayed_messages,
            "alignment_skipped": alignment_skipped,
            "alignment": {
                "desired_delay_ms": entry.session.session_buffer.desired_delay_ms,
                "tf_alignment_window_ms": entry.session.tf_alignment_window_ms,
                "now_unix_ms": now_ms,
                "target_capture_unix_ms": target_capture_unix_ms,
            },
            "messages": messages,
        }))
    }
}

pub fn resolve_profile(profile: &str) -> (String, ProjectionDefaults) {
    let requested = profile.trim().to_lowercase();
    for item in builtin_profiles() {
        if item.name == requested {
            return (item.name.to_string(), item.defaults);
        }
    }

    for item in builtin_profiles() {
        if item.name == "balanced" {
            return (item.name.to_string(), item.defaults);
        }
    }

    (
        "balanced".to_string(),
        ProjectionDefaults {
            point_stride: Some(4),
            voxel_leaf_m: Some(0.08),
            image_scale: Some(0.75),
            hz_limit: Some(8.0),
            desired_delay_ms: 80,
            tf_alignment_window_ms: 250,
        },
    )
}

async fn run_projection_worker(
    sessions: Arc<Mutex<HashMap<String, RunningProjectionSession>>>,
    session_id: String,
    mut stop_rx: watch::Receiver<bool>,
    topics: Vec<String>,
    filters: ProjectionFilters,
) {
    if topics.is_empty() {
        return;
    }

    let mut topic_type_cache: HashMap<String, String> = HashMap::new();

    loop {
        if *stop_rx.borrow() {
            break;
        }

        for topic in &topics {
            if *stop_rx.borrow() {
                break;
            }

            let preferred_type = topic_type_cache.get(topic).map(String::as_str);
            let sampled = sample_topic_projection(topic, &filters, preferred_type).await;
            if let Ok(sample) = sampled.as_ref() {
                if let Some(topic_type) = sample
                    .topic_type
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                {
                    topic_type_cache.insert(topic.clone(), topic_type.to_string());
                }
            }
            let now = unix_now();
            let now_ms = unix_now_ms();
            let mut guard = sessions.lock().await;
            let Some(entry) = guard.get_mut(&session_id) else {
                return;
            };

            entry.session.updated_at_unix = now;
            entry.session.runtime.last_tick_unix = Some(now);
            match sampled {
                Ok(sample) => apply_topic_success(&mut entry.session, topic, sample, now, now_ms),
                Err(err) => apply_topic_failure(&mut entry.session.runtime, topic, err, now),
            }
        }

        let interval = sample_interval(filters.hz_limit);
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }

    let mut guard = sessions.lock().await;
    if let Some(entry) = guard.get_mut(&session_id) {
        entry.session.runtime.worker_state = "stopped".to_string();
        entry.session.updated_at_unix = unix_now();
    }
}

fn build_runtime_view_for_session(
    session: &ProjectionSession,
    topic: &str,
    window_sec: u64,
    now_unix_ms: u64,
) -> Option<TopicRuntimeView> {
    let resolved = resolve_topic_alias(session, topic)?;
    let stats = find_topic_stats(&session.runtime, &resolved.source_topic)?;
    let estimated =
        estimate_runtime_stats(session, &resolved.source_topic, window_sec, now_unix_ms);
    Some(TopicRuntimeView {
        requested_topic: resolved.requested_topic,
        source_topic: resolved.source_topic,
        projected_topic: resolved.projected_topic,
        route_status: resolved.route_status,
        stats,
        estimated,
    })
}

fn resolve_topic_alias(session: &ProjectionSession, topic: &str) -> Option<TopicAliasResolution> {
    let requested = topic.trim();
    if requested.is_empty() {
        return None;
    }

    if let Some(route) = session
        .topic_routes
        .iter()
        .find(|route| route.source_topic == requested)
    {
        return Some(TopicAliasResolution {
            requested_topic: requested.to_string(),
            source_topic: route.source_topic.clone(),
            projected_topic: Some(route.projected_topic.clone()),
            route_status: route.status.clone(),
        });
    }

    if let Some(route) = session
        .topic_routes
        .iter()
        .find(|route| route.projected_topic == requested)
    {
        return Some(TopicAliasResolution {
            requested_topic: requested.to_string(),
            source_topic: route.source_topic.clone(),
            projected_topic: Some(route.projected_topic.clone()),
            route_status: route.status.clone(),
        });
    }

    if session.topics.iter().any(|item| item == requested)
        || session
            .runtime
            .topic_stats
            .iter()
            .any(|item| item.topic == requested)
    {
        let projected = session
            .topic_routes
            .iter()
            .find(|route| route.source_topic == requested)
            .map(|route| route.projected_topic.clone());
        return Some(TopicAliasResolution {
            requested_topic: requested.to_string(),
            source_topic: requested.to_string(),
            projected_topic: projected,
            route_status: "unknown".to_string(),
        });
    }

    None
}

fn select_aligned_messages(
    messages: &[StreamMessage],
    now_unix_ms: u64,
    desired_delay_ms: u64,
    tf_alignment_window_ms: u64,
    limit: usize,
) -> (Vec<StreamMessage>, usize, usize) {
    if messages.is_empty() {
        return (Vec::new(), 0, 0);
    }

    let ready = messages
        .iter()
        .filter(|msg| message_display_at_unix_ms(msg, desired_delay_ms) <= now_unix_ms)
        .cloned()
        .collect::<Vec<_>>();
    if ready.is_empty() {
        return (Vec::new(), messages.len(), 0);
    }

    let target_capture = now_unix_ms.saturating_sub(desired_delay_ms);
    let aligned = if tf_alignment_window_ms == 0 {
        ready
            .iter()
            .filter(|msg| message_capture_unix_ms(msg) <= target_capture)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        let earliest_capture = target_capture.saturating_sub(tf_alignment_window_ms);
        ready
            .iter()
            .filter(|msg| {
                let captured = message_capture_unix_ms(msg);
                captured >= earliest_capture && captured <= target_capture
            })
            .cloned()
            .collect::<Vec<_>>()
    };

    let mut selected = if aligned.is_empty() {
        // Sparse streams can miss the strict alignment window; keep the latest
        // ready sample so UI still progresses without skipping visibility.
        ready.last().cloned().into_iter().collect::<Vec<_>>()
    } else {
        aligned
    };

    let limit = limit.max(1);
    if selected.len() > limit {
        let start = selected.len().saturating_sub(limit);
        selected = selected.into_iter().skip(start).collect();
    }

    let alignment_skipped = ready.len().saturating_sub(selected.len());
    let delayed_messages = messages.len().saturating_sub(ready.len());
    (selected, delayed_messages, alignment_skipped)
}

fn estimate_runtime_stats(
    session: &ProjectionSession,
    topic: &str,
    window_sec: u64,
    now_unix_ms: u64,
) -> RuntimeEstimatedStats {
    let fallback = RuntimeEstimatedStats {
        sample_count: 0,
        sample_window_sec: window_sec as f64,
        average_hz: None,
        average_bw: None,
        average_bw_bps: None,
        average_delay_sec: Some(session.session_buffer.desired_delay_ms as f64 / 1000.0),
        collector_sparse_hint: true,
    };

    let Some(samples) = session.session_buffer.pull_messages_since(topic, None) else {
        return fallback;
    };
    if samples.is_empty() {
        return fallback;
    }

    let window_start_ms = now_unix_ms.saturating_sub(window_sec.saturating_mul(1000));
    let mut in_window = samples
        .iter()
        .filter(|msg| message_capture_unix_ms(msg) >= window_start_ms)
        .cloned()
        .collect::<Vec<_>>();
    if in_window.is_empty() {
        if let Some(last) = samples.last().cloned() {
            in_window.push(last);
        }
    }
    if in_window.is_empty() {
        return fallback;
    }

    let sample_count = in_window.len();
    let first_ms = message_capture_unix_ms(&in_window[0]);
    let last_ms = in_window
        .last()
        .map(message_capture_unix_ms)
        .unwrap_or(first_ms);
    let bytes_sum = in_window.iter().map(|msg| msg.output_bytes).sum::<u64>() as f64;

    let (average_hz, average_bw_bps, sample_window_sec) = if sample_count >= 2 && last_ms > first_ms
    {
        let span = ((last_ms - first_ms) as f64 / 1000.0).max(0.001);
        (
            Some((sample_count.saturating_sub(1)) as f64 / span),
            Some(bytes_sum / span),
            span,
        )
    } else {
        let age = ((now_unix_ms.saturating_sub(last_ms)) as f64 / 1000.0).max(0.0);
        let span = age.max(window_sec as f64).max(0.001);
        (Some(1.0 / span), Some(bytes_sum / span), span)
    };

    RuntimeEstimatedStats {
        sample_count,
        sample_window_sec,
        average_hz,
        average_bw: average_bw_bps.map(format_bytes_per_sec),
        average_bw_bps,
        average_delay_sec: Some(session.session_buffer.desired_delay_ms as f64 / 1000.0),
        collector_sparse_hint: sample_count < 2,
    }
}

fn apply_topic_success(
    session: &mut ProjectionSession,
    topic: &str,
    sample: TopicProjectionSample,
    now_unix: u64,
    now_unix_ms: u64,
) {
    let runtime = &mut session.runtime;
    let idx = ensure_topic_index(runtime, topic);
    let stat = &mut runtime.topic_stats[idx];
    stat.status = "ok".to_string();
    stat.last_sample_status = Some("ok".to_string());
    stat.topic_type = sample.topic_type.clone();
    stat.captures = stat.captures.saturating_add(1);
    stat.last_capture_unix = Some(now_unix);
    stat.last_success_age = Some(0);
    stat.last_error = None;
    stat.input_bytes = Some(sample.input_bytes);
    stat.output_bytes = Some(sample.output_bytes);
    stat.input_points = sample.input_points;
    stat.output_points = sample.output_points;
    stat.input_width_px = sample.input_width_px;
    stat.input_height_px = sample.input_height_px;
    stat.output_width_px = sample.output_width_px;
    stat.output_height_px = sample.output_height_px;
    stat.applied_filters = sample.applied_filters.clone();

    runtime.total_captures = runtime.total_captures.saturating_add(1);
    let desired_delay_ms = session.session_buffer.desired_delay_ms;
    session.session_buffer.push_message(
        topic,
        build_stream_message(topic, sample, now_unix, now_unix_ms, desired_delay_ms),
    );
}

fn apply_topic_failure(
    runtime: &mut ProjectionRuntimeState,
    topic: &str,
    message: String,
    now_unix: u64,
) {
    let idx = ensure_topic_index(runtime, topic);
    let stat = &mut runtime.topic_stats[idx];
    stat.last_sample_status = Some("error".to_string());
    stat.failures = stat.failures.saturating_add(1);
    stat.last_failure_unix = Some(now_unix);
    if let Some(last_success_unix) = stat.last_capture_unix {
        stat.status = "ok_with_stale".to_string();
        stat.last_success_age = Some(now_unix.saturating_sub(last_success_unix));
    } else {
        stat.status = "error".to_string();
        stat.last_success_age = None;
    }
    stat.last_error = Some(message);

    runtime.total_failures = runtime.total_failures.saturating_add(1);
}

fn refresh_success_ages(runtime: &mut ProjectionRuntimeState, now_unix: u64) {
    for stat in &mut runtime.topic_stats {
        stat.last_success_age = stat
            .last_capture_unix
            .map(|last_success| now_unix.saturating_sub(last_success));
    }
}

fn session_status_view(session: &ProjectionSession) -> serde_json::Value {
    let mut out = serde_json::to_value(session).unwrap_or_else(|_| serde_json::json!({}));
    let endpoint_statuses = build_endpoint_statuses(session);
    let runtime_combined_view = build_runtime_combined_view(session);
    if let Some(obj) = out.as_object_mut() {
        obj.insert("endpoint_statuses".to_string(), endpoint_statuses);
        obj.insert("runtime_combined_view".to_string(), runtime_combined_view);
    }
    out
}

fn build_endpoint_statuses(session: &ProjectionSession) -> serde_json::Value {
    let mut out = Vec::new();
    if let Some(url) = session.endpoints.foxglove_ws.as_deref() {
        let bridge = session
            .bridge_services
            .iter()
            .find(|svc| svc.service == "foxglove_bridge");
        out.push(serde_json::json!({
            "endpoint": "foxglove",
            "url": url,
            "readiness": bridge.map(|v| v.status.as_str()).unwrap_or("unknown"),
            "service": "foxglove_bridge",
            "message": bridge.and_then(|v| v.message.clone()),
        }));
    }
    if let Some(url) = session.endpoints.rosbridge_ws.as_deref() {
        let bridge = session
            .bridge_services
            .iter()
            .find(|svc| svc.service == "rosbridge");
        out.push(serde_json::json!({
            "endpoint": "rosbridge",
            "url": url,
            "readiness": bridge.map(|v| v.status.as_str()).unwrap_or("unknown"),
            "service": "rosbridge",
            "message": bridge.and_then(|v| v.message.clone()),
        }));
    }
    if let Some(url) = session.endpoints.rviz_vnc.as_deref() {
        let bridge = session
            .bridge_services
            .iter()
            .find(|svc| svc.service == "rviz_vnc");
        out.push(serde_json::json!({
            "endpoint": "rviz_vnc",
            "url": url,
            "readiness": bridge.map(|v| v.status.as_str()).unwrap_or("unknown"),
            "service": "rviz_vnc",
            "message": bridge
                .and_then(|v| v.message.clone())
                .or_else(|| Some("rviz vnc service status unavailable".to_string())),
        }));
    }
    serde_json::Value::Array(out)
}

fn build_runtime_combined_view(session: &ProjectionSession) -> serde_json::Value {
    let topics = session
        .runtime
        .topic_stats
        .iter()
        .map(|stat| {
            let route = session
                .topic_routes
                .iter()
                .find(|route| route.source_topic == stat.topic);
            let projected_topic = route.map(|route| route.projected_topic.clone());
            let applied_filters = if stat.applied_filters.is_empty() {
                route
                    .map(|route| route.applied_filters.clone())
                    .unwrap_or_default()
            } else {
                stat.applied_filters.clone()
            };
            serde_json::json!({
                "topic": stat.topic,
                "source_topic": stat.topic,
                "display_topic": stat.topic,
                "transport_topic": projected_topic.clone(),
                "projected_topic": projected_topic,
                "status": stat.status,
                "last_success_age": stat.last_success_age,
                "captures": stat.captures,
                "failures": stat.failures,
                "last_error": stat.last_error,
                "route_status": route.map(|route| route.status.as_str()).unwrap_or("unknown"),
                "applied_filters": applied_filters,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "desired_delay_ms": session.session_buffer.desired_delay_ms,
        "tf_alignment_window_ms": session.tf_alignment_window_ms,
        "topics": topics,
    })
}

fn ensure_topic_index(runtime: &mut ProjectionRuntimeState, topic: &str) -> usize {
    if let Some(idx) = runtime
        .topic_stats
        .iter()
        .position(|item| item.topic == topic)
    {
        return idx;
    }
    runtime.topic_stats.push(TopicProjectionStats {
        topic: topic.to_string(),
        status: "pending".to_string(),
        ..Default::default()
    });
    runtime.topic_stats.len() - 1
}

fn find_topic_stats(runtime: &ProjectionRuntimeState, topic: &str) -> Option<TopicProjectionStats> {
    runtime
        .topic_stats
        .iter()
        .find(|item| item.topic == topic)
        .cloned()
}

fn normalize_topics(input: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for topic in input {
        let topic = topic.trim();
        if topic.is_empty() {
            continue;
        }
        if out.iter().any(|existing| existing == topic) {
            continue;
        }
        out.push(topic.to_string());
    }
    out
}

fn sample_interval(hz_limit: Option<f64>) -> Duration {
    let secs = match hz_limit {
        Some(v) if v > 0.0 => (1.0 / v).clamp(0.25, 6.0),
        _ => 1.0,
    };
    Duration::from_millis((secs * 1000.0) as u64)
}

fn build_stream_message(
    topic: &str,
    sample: TopicProjectionSample,
    now_unix: u64,
    now_unix_ms: u64,
    desired_delay_ms: u64,
) -> StreamMessage {
    StreamMessage {
        seq: 0,
        topic: topic.to_string(),
        topic_type: sample.topic_type,
        captured_at_unix: now_unix,
        captured_at_unix_ms: now_unix_ms,
        display_at_unix_ms: now_unix_ms.saturating_add(desired_delay_ms),
        input_bytes: sample.input_bytes,
        output_bytes: sample.output_bytes,
        input_points: sample.input_points,
        output_points: sample.output_points,
        input_width_px: sample.input_width_px,
        input_height_px: sample.input_height_px,
        output_width_px: sample.output_width_px,
        output_height_px: sample.output_height_px,
        input_preview: sample.input_preview,
        output_preview: sample.output_preview,
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn message_capture_unix_ms(msg: &StreamMessage) -> u64 {
    if msg.captured_at_unix_ms > 0 {
        msg.captured_at_unix_ms
    } else {
        msg.captured_at_unix.saturating_mul(1000)
    }
}

fn message_display_at_unix_ms(msg: &StreamMessage, desired_delay_ms: u64) -> u64 {
    if msg.display_at_unix_ms > 0 {
        msg.display_at_unix_ms
    } else {
        message_capture_unix_ms(msg).saturating_add(desired_delay_ms)
    }
}

fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    if !bytes_per_sec.is_finite() || bytes_per_sec <= 0.0 {
        return "0 B/s".to_string();
    }
    let units = ["B/s", "KB/s", "MB/s", "GB/s"];
    let mut value = bytes_per_sec;
    let mut idx = 0usize;
    while value >= 1024.0 && idx + 1 < units.len() {
        value /= 1024.0;
        idx += 1;
    }
    format!("{:.2} {}", value, units[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_topics_deduplicates_and_trims() {
        let topics = vec![
            " /scan ".to_string(),
            "".to_string(),
            "/scan".to_string(),
            "/camera/image".to_string(),
        ];
        let got = normalize_topics(topics);
        assert_eq!(got, vec!["/scan".to_string(), "/camera/image".to_string()]);
    }

    #[test]
    fn sample_interval_uses_hz_limit() {
        let fast = sample_interval(Some(10.0));
        let slow = sample_interval(Some(0.5));
        assert!(fast < slow);
    }

    #[test]
    fn failure_after_success_marks_topic_as_ok_with_stale() {
        let mut session = ProjectionSession {
            session_id: "s1".to_string(),
            session_name: None,
            status: "running".to_string(),
            created_at_unix: 0,
            updated_at_unix: 0,
            mode: ProjectionMode::StatsOnly,
            transport_policy: "auto".to_string(),
            profile: "balanced".to_string(),
            topics: vec!["/map".to_string()],
            tf_alignment_window_ms: 250,
            filters: ProjectionFilters::default(),
            topic_policy: TopicPolicySet::builtin_defaults(),
            session_buffer: SessionLocalBuffer::new(80),
            endpoints: ProjectionEndpoints::default(),
            bridge_services: Vec::new(),
            topic_routes: Vec::new(),
            notes: Vec::new(),
            runtime: ProjectionRuntimeState::new(&["/map".to_string()]),
        };

        let sample = TopicProjectionSample {
            topic_type: Some("nav_msgs/msg/OccupancyGrid".to_string()),
            input_bytes: 128,
            output_bytes: 128,
            input_points: None,
            output_points: None,
            input_width_px: None,
            input_height_px: None,
            output_width_px: None,
            output_height_px: None,
            applied_filters: Vec::new(),
            input_preview: None,
            output_preview: None,
        };

        apply_topic_success(&mut session, "/map", sample, 100, 100_000);
        apply_topic_failure(
            &mut session.runtime,
            "/map",
            "timeout after 8s".to_string(),
            112,
        );

        let stat = session
            .runtime
            .topic_stats
            .iter()
            .find(|item| item.topic == "/map")
            .expect("topic stat");
        assert_eq!(stat.status, "ok_with_stale");
        assert_eq!(stat.last_capture_unix, Some(100));
        assert_eq!(stat.last_failure_unix, Some(112));
        assert_eq!(stat.last_success_age, Some(12));
        assert_eq!(stat.last_sample_status.as_deref(), Some("error"));
    }

    #[test]
    fn failure_without_success_stays_error() {
        let mut runtime = ProjectionRuntimeState::new(&["/map".to_string()]);
        apply_topic_failure(&mut runtime, "/map", "timeout after 8s".to_string(), 20);
        let stat = runtime
            .topic_stats
            .iter()
            .find(|item| item.topic == "/map")
            .expect("topic stat");
        assert_eq!(stat.status, "error");
        assert_eq!(stat.last_capture_unix, None);
        assert_eq!(stat.last_failure_unix, Some(20));
        assert_eq!(stat.last_success_age, None);
    }

    #[test]
    fn refresh_success_ages_updates_age_from_last_success() {
        let mut runtime = ProjectionRuntimeState::new(&["/map".to_string()]);
        let idx = ensure_topic_index(&mut runtime, "/map");
        runtime.topic_stats[idx].last_capture_unix = Some(42);
        runtime.topic_stats[idx].status = "ok_with_stale".to_string();

        refresh_success_ages(&mut runtime, 50);
        assert_eq!(runtime.topic_stats[idx].last_success_age, Some(8));
    }

    #[test]
    fn alignment_prefers_messages_within_target_window() {
        let msgs = vec![
            StreamMessage {
                seq: 1,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 980,
                display_at_unix_ms: 1080,
                input_bytes: 10,
                output_bytes: 10,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
            StreamMessage {
                seq: 2,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 1000,
                display_at_unix_ms: 1100,
                input_bytes: 10,
                output_bytes: 10,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
            StreamMessage {
                seq: 3,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 1040,
                display_at_unix_ms: 1140,
                input_bytes: 10,
                output_bytes: 10,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
        ];

        let (selected, delayed, skipped) = select_aligned_messages(&msgs, 1300, 200, 60, 8);
        assert_eq!(delayed, 0);
        assert_eq!(skipped, 2);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].seq, 3);
    }

    #[test]
    fn resolve_topic_alias_accepts_projected_topic() {
        let session = ProjectionSession {
            session_id: "s1".to_string(),
            session_name: None,
            status: "running".to_string(),
            created_at_unix: 0,
            updated_at_unix: 0,
            mode: ProjectionMode::Foxglove,
            transport_policy: "auto".to_string(),
            profile: "balanced".to_string(),
            topics: vec!["/scan".to_string()],
            tf_alignment_window_ms: 250,
            filters: ProjectionFilters::default(),
            topic_policy: TopicPolicySet::builtin_defaults(),
            session_buffer: SessionLocalBuffer::new(80),
            endpoints: ProjectionEndpoints::default(),
            bridge_services: Vec::new(),
            topic_routes: vec![TopicProjectionRoute {
                source_topic: "/scan".to_string(),
                source_topic_type: Some("sensor_msgs/msg/LaserScan".to_string()),
                projected_topic: "/rt/debug/s1/scan".to_string(),
                projected_topic_type: Some("sensor_msgs/msg/LaserScan".to_string()),
                policy_key: None,
                policy_transform: None,
                status: "running".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: true,
                launch_command: None,
                applied_filters: Vec::new(),
                unsupported_filters: Vec::new(),
                message: None,
                checked_at_unix: 0,
            }],
            notes: Vec::new(),
            runtime: ProjectionRuntimeState::new(&["/scan".to_string()]),
        };

        let resolved = resolve_topic_alias(&session, "/rt/debug/s1/scan").expect("resolve alias");
        assert_eq!(resolved.source_topic, "/scan");
        assert_eq!(
            resolved.projected_topic.as_deref(),
            Some("/rt/debug/s1/scan")
        );
    }

    #[test]
    fn alignment_falls_back_to_latest_ready_sample_when_window_misses() {
        let msgs = vec![
            StreamMessage {
                seq: 1,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 700,
                display_at_unix_ms: 900,
                input_bytes: 10,
                output_bytes: 10,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
            StreamMessage {
                seq: 2,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 760,
                display_at_unix_ms: 960,
                input_bytes: 10,
                output_bytes: 10,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
        ];

        let (selected, delayed, skipped) = select_aligned_messages(&msgs, 1100, 200, 20, 8);
        assert_eq!(delayed, 0);
        assert_eq!(skipped, 1);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].seq, 2);
    }

    #[test]
    fn runtime_estimation_marks_sparse_for_single_sample() {
        let mut session = ProjectionSession {
            session_id: "s1".to_string(),
            session_name: None,
            status: "running".to_string(),
            created_at_unix: 0,
            updated_at_unix: 0,
            mode: ProjectionMode::StatsOnly,
            transport_policy: "auto".to_string(),
            profile: "balanced".to_string(),
            topics: vec!["/scan".to_string()],
            tf_alignment_window_ms: 250,
            filters: ProjectionFilters::default(),
            topic_policy: TopicPolicySet::builtin_defaults(),
            session_buffer: SessionLocalBuffer::new(180),
            endpoints: ProjectionEndpoints::default(),
            bridge_services: Vec::new(),
            topic_routes: Vec::new(),
            notes: Vec::new(),
            runtime: ProjectionRuntimeState::new(&["/scan".to_string()]),
        };

        session.session_buffer.push_message(
            "/scan",
            StreamMessage {
                seq: 0,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: 0,
                captured_at_unix_ms: 1000,
                display_at_unix_ms: 1180,
                input_bytes: 64,
                output_bytes: 32,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            },
        );

        let estimated = estimate_runtime_stats(&session, "/scan", 6, 4000);
        assert_eq!(estimated.sample_count, 1);
        assert!(estimated.collector_sparse_hint);
        let delay = estimated.average_delay_sec.expect("delay");
        assert!((delay - 0.18).abs() < 1e-9);
        assert!(estimated.average_hz.is_some());
    }

    #[test]
    fn runtime_combined_view_prefers_source_display_topic_and_transport_alias() {
        let mut session = ProjectionSession {
            session_id: "s1".to_string(),
            session_name: None,
            status: "running".to_string(),
            created_at_unix: 0,
            updated_at_unix: 0,
            mode: ProjectionMode::Foxglove,
            transport_policy: "auto".to_string(),
            profile: "balanced".to_string(),
            topics: vec!["/scan".to_string()],
            tf_alignment_window_ms: 250,
            filters: ProjectionFilters::default(),
            topic_policy: TopicPolicySet::builtin_defaults(),
            session_buffer: SessionLocalBuffer::new(80),
            endpoints: ProjectionEndpoints::default(),
            bridge_services: Vec::new(),
            topic_routes: vec![TopicProjectionRoute {
                source_topic: "/scan".to_string(),
                source_topic_type: Some("sensor_msgs/msg/LaserScan".to_string()),
                projected_topic: "/rt/debug/s1/scan".to_string(),
                projected_topic_type: Some("sensor_msgs/msg/LaserScan".to_string()),
                policy_key: None,
                policy_transform: None,
                status: "running".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: true,
                launch_command: None,
                applied_filters: Vec::new(),
                unsupported_filters: Vec::new(),
                message: None,
                checked_at_unix: 0,
            }],
            notes: Vec::new(),
            runtime: ProjectionRuntimeState::new(&["/scan".to_string()]),
        };
        let idx = ensure_topic_index(&mut session.runtime, "/scan");
        session.runtime.topic_stats[idx].status = "ok".to_string();

        let view = build_runtime_combined_view(&session);
        let topics = view
            .get("topics")
            .and_then(|v| v.as_array())
            .expect("topics array");
        let first = topics.first().expect("first topic item");
        assert_eq!(
            first
                .get("display_topic")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "/scan"
        );
        assert_eq!(
            first
                .get("source_topic")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "/scan"
        );
        assert_eq!(
            first
                .get("transport_topic")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "/rt/debug/s1/scan"
        );
    }
}
