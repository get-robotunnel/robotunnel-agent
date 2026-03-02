//! Journal log viewer for the Debug Skill.

use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use rt_runtime::executor;

/// Handle a "logs" action.
///
/// Params:
///   - unit (string, optional): systemd unit name to filter logs (e.g., "robotunnel")
///   - lines (number, optional): Number of lines to return (default: 50)
///   - since (string, optional): Time filter, e.g. "1h", "30m", "2024-01-01"
pub async fn handle(request: CommandRequest) -> CommandResponse {
    let unit = request.params.get("unit").and_then(|v| v.as_str());
    let lines = request
        .params
        .get("lines")
        .and_then(|v| v.as_u64())
        .unwrap_or(50);
    let since = request.params.get("since").and_then(|v| v.as_str());

    let mut cmd = String::from("journalctl --no-pager");

    if let Some(unit) = unit {
        // Sanitize unit name to prevent injection
        let safe_unit: String = unit
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_' || *c == '.')
            .collect();
        cmd.push_str(&format!(" -u {}", safe_unit));
    }

    cmd.push_str(&format!(" -n {}", lines.min(1000)));

    if let Some(since) = since {
        // Sanitize since value
        let safe_since: String = since
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == ':' || *c == ' ' || *c == 'T')
            .collect();
        cmd.push_str(&format!(" --since \"{}\"", safe_since));
    }

    match executor::exec_command(&cmd, 10, 256 * 1024).await {
        Ok(result) => CommandResponse {
            id: request.id,
            status: CommandStatus::Ok,
            data: Some(serde_json::json!({
                "logs": result.stdout,
                "lines_requested": lines,
            })),
            error: if result.stderr.is_empty() {
                None
            } else {
                Some(result.stderr)
            },
        },
        Err(e) => CommandResponse {
            id: request.id,
            status: CommandStatus::Error,
            data: None,
            error: Some(format!("failed to read logs: {}", e)),
        },
    }
}
