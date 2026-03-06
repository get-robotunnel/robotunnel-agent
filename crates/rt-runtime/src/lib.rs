//! Shared dispatch primitives for the RoboTunnel agent application layer.

pub mod executor;
pub mod router;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Debug, Error)]
pub enum SkillError {
    #[error("Action not found: {0}")]
    ActionNotFound(String),
    #[error("Invalid parameters: {0}")]
    InvalidParams(String),
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    #[error("Execution timeout: {0}s")]
    Timeout(u64),
}

pub type ExecutionResult = Result<Value, SkillError>;

#[async_trait]
pub trait Skill: Send + Sync {
    /// Name of the skill (e.g., "debug", "ros2").
    fn name(&self) -> &str;

    /// Execute an action within this skill.
    /// Takes the broadcast channel for "push" data streaming.
    async fn execute(
        &self,
        action: &str,
        params: Value,
        broadcast_tx: broadcast::Sender<Vec<u8>>,
    ) -> ExecutionResult;
}
