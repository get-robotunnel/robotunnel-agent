use super::adapters::{build_endpoints, ProjectionEndpoints};
use super::buffers::SessionLocalBuffer;
use super::profiles::{builtin_profiles, ProjectionDefaults};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
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
    pub session_buffer: SessionLocalBuffer,
    pub endpoints: ProjectionEndpoints,
    pub notes: Vec<String>,
}

pub struct ProjectionEngine {
    sessions: Mutex<HashMap<String, ProjectionSession>>,
}

impl ProjectionEngine {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
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

        let session = ProjectionSession {
            session_id: session_id.clone(),
            session_name: req.session_name,
            status: "running".to_string(),
            created_at_unix: now,
            updated_at_unix: now,
            mode: req.mode,
            transport_policy: req.transport_policy,
            profile: req.profile,
            topics: req.topics,
            filters: req.filters,
            session_buffer: SessionLocalBuffer::new(req.desired_delay_ms),
            endpoints,
            notes: vec![
                "projection configured session-locally; production subscribers remain unchanged"
                    .to_string(),
                "use robotunnel connect with tcp-only route preference for deterministic debug transport"
                    .to_string(),
            ],
        };

        let mut guard = self.sessions.lock().await;
        guard.insert(session_id, session.clone());
        session
    }

    pub async fn stop_session(&self, session_id: &str) -> Option<ProjectionSession> {
        let mut guard = self.sessions.lock().await;
        let mut session = guard.remove(session_id)?;
        session.status = "stopped".to_string();
        session.updated_at_unix = unix_now();
        Some(session)
    }

    pub async fn status(&self, session_id: Option<&str>) -> serde_json::Value {
        let guard = self.sessions.lock().await;
        if let Some(id) = session_id {
            if let Some(session) = guard.get(id) {
                return serde_json::json!({"session": session});
            }
            return serde_json::json!({"session": serde_json::Value::Null});
        }

        let sessions = guard.values().cloned().collect::<Vec<_>>();
        serde_json::json!({
            "count": sessions.len(),
            "sessions": sessions,
        })
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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
