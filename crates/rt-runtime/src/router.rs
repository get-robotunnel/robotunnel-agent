//! Application router — dispatches incoming commands to registered skill handlers.

use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use rt_core::tunnel::IncomingCommand;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing;

/// A skill handler function that processes a command request and returns a response.
/// Takes a broadcast sender for "push" data (e.g. ROS2 topics).
pub type SkillHandler = Arc<
    dyn Fn(
            CommandRequest,
            broadcast::Sender<Vec<u8>>,
        ) -> Pin<Box<dyn Future<Output = CommandResponse> + Send>>
        + Send
        + Sync,
>;

/// Routes incoming commands to registered RoboTunnel skill handlers.
pub struct Router {
    skills: HashMap<String, SkillHandler>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
}

impl Router {
    pub fn new(broadcast_tx: broadcast::Sender<Vec<u8>>) -> Self {
        Self {
            skills: HashMap::new(),
            broadcast_tx,
        }
    }

    /// Register a skill handler.
    pub fn register<F, Fut>(&mut self, skill_name: &str, handler: F)
    where
        F: Fn(CommandRequest, broadcast::Sender<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CommandResponse> + Send + 'static,
    {
        let _broadcast_tx = self.broadcast_tx.clone();
        self.skills.insert(
            skill_name.to_string(),
            Arc::new(move |req, tx| Box::pin(handler(req, tx))),
        );
    }

    /// Run the router, consuming commands from the channel.
    pub async fn run(&self, mut rx: mpsc::Receiver<IncomingCommand>) {
        tracing::info!(
            "router: started with {} skill(s) registered",
            self.skills.len()
        );

        while let Some(incoming) = rx.recv().await {
            let skill_name = &incoming.request.skill;

            let response = if let Some(handler) = self.skills.get(skill_name) {
                tracing::debug!(
                    "router: dispatching {}.{} (id={})",
                    skill_name,
                    incoming.request.action,
                    incoming.request.id
                );
                handler(incoming.request, self.broadcast_tx.clone()).await
            } else {
                tracing::warn!("router: unknown skill '{}'", skill_name);
                CommandResponse {
                    id: incoming.request.id,
                    status: CommandStatus::Error,
                    data: None,
                    error: Some(format!("unknown skill: {}", skill_name)),
                }
            };

            if incoming.response_tx.send(response).await.is_err() {
                tracing::warn!("router: response channel closed");
            }
        }

        tracing::info!("router: command channel closed, shutting down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_router_dispatches_to_skill() {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(16);
        let mut router = Router::new(broadcast_tx);
        router.register("echo", |req: CommandRequest, _tx| async move {
            CommandResponse {
                id: req.id,
                status: CommandStatus::Ok,
                data: Some(req.params),
                error: None,
            }
        });

        let (tx, rx) = mpsc::channel(16);
        let router_handle = tokio::spawn(async move {
            router.run(rx).await;
        });

        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        let cmd = IncomingCommand {
            request: CommandRequest {
                id: "test-1".to_string(),
                skill: "echo".to_string(),
                action: "ping".to_string(),
                params: json!({"msg": "hello"}),
            },
            response_tx: resp_tx,
        };

        tx.send(cmd).await.unwrap();
        let resp = resp_rx.recv().await.unwrap();
        assert_eq!(resp.status, CommandStatus::Ok);
        assert_eq!(resp.data.unwrap()["msg"], "hello");

        drop(tx); // Close channel to stop router
        let _ = router_handle.await;
    }

    #[tokio::test]
    async fn test_router_unknown_skill() {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(16);
        let router = Router::new(broadcast_tx); // No skills registered

        let (tx, rx) = mpsc::channel(16);
        let router_handle = tokio::spawn(async move {
            router.run(rx).await;
        });

        let (resp_tx, mut resp_rx) = mpsc::channel(1);
        let cmd = IncomingCommand {
            request: CommandRequest {
                id: "test-2".to_string(),
                skill: "nonexistent".to_string(),
                action: "do".to_string(),
                params: json!({}),
            },
            response_tx: resp_tx,
        };

        tx.send(cmd).await.unwrap();
        let resp = resp_rx.recv().await.unwrap();
        assert_eq!(resp.status, CommandStatus::Error);
        assert!(resp.error.unwrap().contains("nonexistent"));

        drop(tx);
        let _ = router_handle.await;
    }
}
