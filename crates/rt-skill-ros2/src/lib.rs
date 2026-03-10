use async_trait::async_trait;
use rt_agent_dispatch::{ExecutionResult, Skill, SkillError};
use serde_json::Value;
use tokio::sync::broadcast;

pub struct Ros2Skill {
    bridge_url: String,
}

impl Ros2Skill {
    pub fn new(bridge_url: &str) -> Self {
        Self {
            bridge_url: bridge_url.to_string(),
        }
    }
}

#[async_trait]
impl Skill for Ros2Skill {
    fn name(&self) -> &str {
        "ros2"
    }

    async fn execute(
        &self,
        _action: &str,
        _params: Value,
        _broadcast_tx: broadcast::Sender<Vec<u8>>,
    ) -> ExecutionResult {
        match _action {
            "list_topics" => Ok(serde_json::json!(["/cmd_vel", "/odom", "/scan", "/tf"])),
            "topic_info" => Ok(serde_json::json!({
                "topic": _params["topic"],
                "type": "geometry_msgs/msg/Twist",
                "bridge_url": self.bridge_url,
            })),
            "subscribe" => {
                Ok(serde_json::json!({
                    "status": "mock_streaming",
                    "topic": _params["topic"],
                    "bridge_url": self.bridge_url,
                }))
            }
            _ => Err(SkillError::ActionNotFound(_action.to_string())),
        }
    }
}
