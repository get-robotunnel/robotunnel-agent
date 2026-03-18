use super::adapters::ProjectionEndpoints;
use super::engine::ProjectionMode;
use serde::{Deserialize, Serialize};
use std::process::Stdio;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeServiceState {
    pub service: String,
    pub endpoint: String,
    pub port: u16,
    pub status: String,
    pub ownership: String,
    pub launch_attempted: bool,
    pub launch_command: Option<String>,
    pub message: Option<String>,
    pub checked_at_unix: u64,
}

pub struct BridgeProcessHandle {
    child: Child,
}

impl BridgeProcessHandle {
    pub async fn stop(&mut self) {
        if self.child.id().is_none() {
            return;
        }

        let _ = self.child.start_kill();
        let _ = timeout(Duration::from_secs(2), self.child.wait()).await;
    }
}

pub async fn ensure_mode_bridges(
    mode: ProjectionMode,
    endpoints: &ProjectionEndpoints,
) -> (
    Vec<BridgeServiceState>,
    Vec<BridgeProcessHandle>,
    Vec<String>,
) {
    match mode {
        ProjectionMode::Foxglove => ensure_foxglove_mode_bridges(endpoints).await,
        ProjectionMode::RvizVnc | ProjectionMode::StatsOnly => (Vec::new(), Vec::new(), Vec::new()),
    }
}

async fn ensure_foxglove_mode_bridges(
    endpoints: &ProjectionEndpoints,
) -> (
    Vec<BridgeServiceState>,
    Vec<BridgeProcessHandle>,
    Vec<String>,
) {
    let mut states = Vec::new();
    let mut handles = Vec::new();
    let mut notes = Vec::new();

    if let Some(endpoint) = endpoints.foxglove_ws.as_deref() {
        let ensured = ensure_foxglove_service(endpoint).await;
        if ensured.status.status != "running" {
            notes.push(format!(
                "foxglove bridge not ready at {}: {}",
                ensured.status.endpoint,
                ensured.status.message.as_deref().unwrap_or("unreachable")
            ));
        } else if ensured.status.ownership == "session_managed" {
            notes.push(format!(
                "foxglove bridge launched for session at {}",
                ensured.status.endpoint
            ));
        } else {
            notes.push(format!(
                "foxglove bridge already available at {}",
                ensured.status.endpoint
            ));
        }
        states.push(ensured.status);
        if let Some(handle) = ensured.handle {
            handles.push(handle);
        }
    }

    if let Some(endpoint) = endpoints.rosbridge_ws.as_deref() {
        let port = parse_endpoint_port(endpoint).unwrap_or(9090);
        let reachable = is_local_port_reachable(port).await;
        states.push(BridgeServiceState {
            service: "rosbridge".to_string(),
            endpoint: endpoint.to_string(),
            port,
            status: if reachable {
                "running".to_string()
            } else {
                "unreachable".to_string()
            },
            ownership: "external".to_string(),
            launch_attempted: false,
            launch_command: None,
            message: if reachable {
                Some("rosbridge endpoint reachable".to_string())
            } else {
                Some("rosbridge endpoint not reachable (not auto-started)".to_string())
            },
            checked_at_unix: unix_now(),
        });
    }

    (states, handles, notes)
}

struct EnsuredBridge {
    status: BridgeServiceState,
    handle: Option<BridgeProcessHandle>,
}

async fn ensure_foxglove_service(endpoint: &str) -> EnsuredBridge {
    let endpoint = endpoint.trim();
    let now = unix_now();
    let port = parse_endpoint_port(endpoint).unwrap_or(8765);

    if is_local_port_reachable(port).await {
        return EnsuredBridge {
            status: BridgeServiceState {
                service: "foxglove_bridge".to_string(),
                endpoint: endpoint.to_string(),
                port,
                status: "running".to_string(),
                ownership: "external".to_string(),
                launch_attempted: false,
                launch_command: None,
                message: Some("existing process detected on endpoint".to_string()),
                checked_at_unix: now,
            },
            handle: None,
        };
    }

    let command = format!(
        "exec ros2 run foxglove_bridge foxglove_bridge --ros-args -p port:={}",
        port
    );
    let mut cmd = Command::new("sh");
    cmd.args(["-lc", &command]);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return EnsuredBridge {
                status: BridgeServiceState {
                    service: "foxglove_bridge".to_string(),
                    endpoint: endpoint.to_string(),
                    port,
                    status: "error".to_string(),
                    ownership: "session_managed".to_string(),
                    launch_attempted: true,
                    launch_command: Some(command),
                    message: Some(format!("spawn failed: {}", err)),
                    checked_at_unix: unix_now(),
                },
                handle: None,
            };
        }
    };

    for _ in 0..10 {
        if is_local_port_reachable(port).await {
            return EnsuredBridge {
                status: BridgeServiceState {
                    service: "foxglove_bridge".to_string(),
                    endpoint: endpoint.to_string(),
                    port,
                    status: "running".to_string(),
                    ownership: "session_managed".to_string(),
                    launch_attempted: true,
                    launch_command: Some(command.clone()),
                    message: Some("bridge launched by visual_debug session".to_string()),
                    checked_at_unix: unix_now(),
                },
                handle: Some(BridgeProcessHandle { child }),
            };
        }

        match child.try_wait() {
            Ok(Some(exit_status)) => {
                return EnsuredBridge {
                    status: BridgeServiceState {
                        service: "foxglove_bridge".to_string(),
                        endpoint: endpoint.to_string(),
                        port,
                        status: "error".to_string(),
                        ownership: "session_managed".to_string(),
                        launch_attempted: true,
                        launch_command: Some(command),
                        message: Some(format!("bridge exited early with {}", exit_status)),
                        checked_at_unix: unix_now(),
                    },
                    handle: None,
                };
            }
            Ok(None) => {}
            Err(err) => {
                return EnsuredBridge {
                    status: BridgeServiceState {
                        service: "foxglove_bridge".to_string(),
                        endpoint: endpoint.to_string(),
                        port,
                        status: "error".to_string(),
                        ownership: "session_managed".to_string(),
                        launch_attempted: true,
                        launch_command: Some(command),
                        message: Some(format!("bridge process check failed: {}", err)),
                        checked_at_unix: unix_now(),
                    },
                    handle: None,
                };
            }
        }

        sleep(Duration::from_millis(200)).await;
    }

    let _ = child.start_kill();
    let _ = timeout(Duration::from_secs(1), child.wait()).await;
    EnsuredBridge {
        status: BridgeServiceState {
            service: "foxglove_bridge".to_string(),
            endpoint: endpoint.to_string(),
            port,
            status: "error".to_string(),
            ownership: "session_managed".to_string(),
            launch_attempted: true,
            launch_command: Some(command),
            message: Some("bridge did not become reachable in time".to_string()),
            checked_at_unix: unix_now(),
        },
        handle: None,
    }
}

fn parse_endpoint_port(endpoint: &str) -> Option<u16> {
    let mut rest = endpoint.trim();
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    let host_part = rest.split('/').next().unwrap_or(rest).trim();
    if host_part.is_empty() {
        return None;
    }
    let (_, port_str) = host_part.rsplit_once(':')?;
    let port = port_str.trim().parse::<u16>().ok()?;
    if port == 0 {
        None
    } else {
        Some(port)
    }
}

async fn is_local_port_reachable(port: u16) -> bool {
    if port == 0 {
        return false;
    }
    matches!(
        timeout(
            Duration::from_millis(700),
            TcpStream::connect(("127.0.0.1", port))
        )
        .await,
        Ok(Ok(_))
    )
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::parse_endpoint_port;

    #[test]
    fn parse_endpoint_port_handles_ws_urls() {
        assert_eq!(parse_endpoint_port("ws://localhost:8765"), Some(8765));
        assert_eq!(
            parse_endpoint_port("ws://127.0.0.1:9090/some/path"),
            Some(9090)
        );
    }
}
