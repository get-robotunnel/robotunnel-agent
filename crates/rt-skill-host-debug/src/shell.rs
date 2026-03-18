//! Shell command execution handler for the Debug Skill.

use rt_agent_dispatch::executor;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_OUTPUT: usize = 64 * 1024; // 64KB

/// Handle a "shell" action.
///
/// Params:
///   - cmd (string, required): The shell command to execute
///   - timeout (number, optional): Timeout in seconds (default: 30)
pub async fn handle(request: CommandRequest) -> CommandResponse {
    if !shell_enabled() {
        return CommandResponse {
            id: request.id,
            status: CommandStatus::Error,
            data: None,
            error: Some(
                "debug.shell is disabled. Set RT_DEBUG_SHELL_ENABLED=true to allow shell commands."
                    .to_string(),
            ),
        };
    }

    let cmd = match request.params.get("cmd").and_then(|v| v.as_str()) {
        Some(cmd) => cmd.to_string(),
        None => {
            return CommandResponse {
                id: request.id,
                status: CommandStatus::Error,
                data: None,
                error: Some("missing required param: cmd".to_string()),
            };
        }
    };

    let timeout = request
        .params
        .get("timeout")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_TIMEOUT_SECS);

    match executor::exec_command(&cmd, timeout, DEFAULT_MAX_OUTPUT).await {
        Ok(result) => CommandResponse {
            id: request.id,
            status: if result.exit_code == Some(0) {
                CommandStatus::Ok
            } else {
                CommandStatus::Error
            },
            data: Some(serde_json::json!({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
            })),
            error: None,
        },
        Err(executor::ExecutorError::Timeout(secs)) => CommandResponse {
            id: request.id,
            status: CommandStatus::Timeout,
            data: None,
            error: Some(format!("command timed out after {}s", secs)),
        },
        Err(e) => CommandResponse {
            id: request.id,
            status: CommandStatus::Error,
            data: None,
            error: Some(format!("execution failed: {}", e)),
        },
    }
}

fn shell_enabled() -> bool {
    match std::env::var("RT_DEBUG_SHELL_ENABLED") {
        Ok(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}
