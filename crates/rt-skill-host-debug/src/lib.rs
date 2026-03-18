pub mod logs;
pub mod shell;
pub mod status;

use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use tokio::sync::broadcast;
use tracing;

/// Handle a debug skill command request.
pub async fn handle(
    request: CommandRequest,
    _broadcast_tx: broadcast::Sender<Vec<u8>>,
) -> CommandResponse {
    tracing::debug!("debug skill: handling action '{}'", request.action);

    match request.action.as_str() {
        "shell" => shell::handle(request).await,
        "logs" => logs::handle(request).await,
        "status" => status::handle(request).await,
        _ => CommandResponse {
            id: request.id,
            status: CommandStatus::Error,
            data: None,
            error: Some(format!("unknown debug action: {}", request.action)),
        },
    }
}
