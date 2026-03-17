use super::engine::ProjectionMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectionEndpoints {
    pub foxglove_ws: Option<String>,
    pub rosbridge_ws: Option<String>,
    pub rviz_vnc: Option<String>,
}

pub fn build_endpoints(mode: ProjectionMode, vnc_port: Option<u16>) -> ProjectionEndpoints {
    match mode {
        ProjectionMode::Foxglove => ProjectionEndpoints {
            foxglove_ws: Some("ws://localhost:8765".to_string()),
            rosbridge_ws: Some("ws://localhost:9090".to_string()),
            rviz_vnc: None,
        },
        ProjectionMode::RvizVnc => ProjectionEndpoints {
            foxglove_ws: None,
            rosbridge_ws: None,
            rviz_vnc: Some(format!("vnc://localhost:{}", vnc_port.unwrap_or(5901))),
        },
        ProjectionMode::StatsOnly => ProjectionEndpoints::default(),
    }
}
