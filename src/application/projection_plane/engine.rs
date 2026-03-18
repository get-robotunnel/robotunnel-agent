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
        let guard = self.sessions.lock().await;
        if let Some(id) = session_id {
            if let Some(entry) = guard.get(id) {
                return serde_json::json!({"session": entry.session});
            }
            return serde_json::json!({"session": serde_json::Value::Null});
        }

        let sessions = guard
            .values()
            .map(|entry| entry.session.clone())
            .collect::<Vec<_>>();
        serde_json::json!({
            "count": sessions.len(),
            "sessions": sessions,
        })
    }

    pub async fn topic_runtime_stats(
        &self,
        topic: &str,
        session_id: Option<&str>,
    ) -> Option<TopicProjectionStats> {
        let topic = topic.trim();
        if topic.is_empty() {
            return None;
        }

        let guard = self.sessions.lock().await;
        if let Some(session_id) = session_id {
            let entry = guard.get(session_id)?;
            return find_topic_stats(&entry.session.runtime, topic);
        }

        let mut best: Option<(u64, TopicProjectionStats)> = None;
        for entry in guard.values() {
            if let Some(stats) = find_topic_stats(&entry.session.runtime, topic) {
                let updated = entry.session.updated_at_unix;
                let replace = best.as_ref().map(|(ts, _)| updated >= *ts).unwrap_or(true);
                if replace {
                    best = Some((updated, stats));
                }
            }
        }
        best.map(|(_, stats)| stats)
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
        let messages = entry
            .session
            .session_buffer
            .pull_messages(topic, since_seq, limit)?;
        let stream_meta = entry.session.session_buffer.topics.get(topic)?;
        Some(serde_json::json!({
            "session_id": session_id,
            "topic": topic,
            "since_seq": since_seq,
            "returned": messages.len(),
            "last_seq": stream_meta.last_seq,
            "dropped_messages": stream_meta.dropped_messages,
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
            let mut guard = sessions.lock().await;
            let Some(entry) = guard.get_mut(&session_id) else {
                return;
            };

            entry.session.updated_at_unix = now;
            entry.session.runtime.last_tick_unix = Some(now);
            match sampled {
                Ok(sample) => apply_topic_success(&mut entry.session, topic, sample, now),
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

fn apply_topic_success(
    session: &mut ProjectionSession,
    topic: &str,
    sample: TopicProjectionSample,
    now_unix: u64,
) {
    let runtime = &mut session.runtime;
    let idx = ensure_topic_index(runtime, topic);
    let stat = &mut runtime.topic_stats[idx];
    stat.status = "ok".to_string();
    stat.topic_type = sample.topic_type.clone();
    stat.captures = stat.captures.saturating_add(1);
    stat.last_capture_unix = Some(now_unix);
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
    session
        .session_buffer
        .push_message(topic, build_stream_message(topic, sample, now_unix));
}

fn apply_topic_failure(
    runtime: &mut ProjectionRuntimeState,
    topic: &str,
    message: String,
    now_unix: u64,
) {
    let idx = ensure_topic_index(runtime, topic);
    let stat = &mut runtime.topic_stats[idx];
    stat.status = "error".to_string();
    stat.failures = stat.failures.saturating_add(1);
    stat.last_capture_unix = Some(now_unix);
    stat.last_error = Some(message);

    runtime.total_failures = runtime.total_failures.saturating_add(1);
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
) -> StreamMessage {
    StreamMessage {
        seq: 0,
        topic: topic.to_string(),
        topic_type: sample.topic_type,
        captured_at_unix: now_unix,
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
}
