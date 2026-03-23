use super::adapters::ProjectionEndpoints;
use super::engine::ProjectionMode;
use rt_core::ros::wrap_ros_shell;
use serde::{Deserialize, Serialize};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const EMBEDDED_RVIZ_VNC_LAUNCHER: &str = include_str!("../../../scripts/rviz_vnc_session.sh");

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
        ProjectionMode::RvizVnc => ensure_rviz_mode_bridges(endpoints).await,
        ProjectionMode::StatsOnly => (Vec::new(), Vec::new(), Vec::new()),
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

async fn ensure_rviz_mode_bridges(
    endpoints: &ProjectionEndpoints,
) -> (
    Vec<BridgeServiceState>,
    Vec<BridgeProcessHandle>,
    Vec<String>,
) {
    let mut states = Vec::new();
    let mut handles = Vec::new();
    let mut notes = Vec::new();

    let Some(endpoint) = endpoints.rviz_vnc.as_deref() else {
        return (states, handles, notes);
    };

    let ensured = ensure_rviz_vnc_service(endpoint).await;
    if ensured.status.status != "running" {
        notes.push(format!(
            "rviz_vnc service not ready at {}: {}",
            ensured.status.endpoint,
            ensured
                .status
                .message
                .as_deref()
                .unwrap_or("unreachable or launch failed")
        ));
    } else if ensured.status.ownership == "session_managed" {
        notes.push(format!(
            "rviz_vnc service launched for session at {}",
            ensured.status.endpoint
        ));
    } else {
        notes.push(format!(
            "rviz_vnc service already available at {}",
            ensured.status.endpoint
        ));
    }
    states.push(ensured.status);
    if let Some(handle) = ensured.handle {
        handles.push(handle);
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
    let wrapped_command = wrap_ros_shell(&command);
    let mut cmd = Command::new("bash");
    cmd.args(["-lc", &wrapped_command]);
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

async fn ensure_rviz_vnc_service(endpoint: &str) -> EnsuredBridge {
    let endpoint = endpoint.trim();
    let port = parse_endpoint_port(endpoint).unwrap_or(5901);

    if is_local_port_reachable(port).await {
        return EnsuredBridge {
            status: BridgeServiceState {
                service: "rviz_vnc".to_string(),
                endpoint: endpoint.to_string(),
                port,
                status: "running".to_string(),
                ownership: "external".to_string(),
                launch_attempted: false,
                launch_command: None,
                message: Some("existing rviz_vnc process detected on endpoint".to_string()),
                checked_at_unix: unix_now(),
            },
            handle: None,
        };
    }

    let Some(command) = build_rviz_vnc_launch_command(port) else {
        return EnsuredBridge {
            status: BridgeServiceState {
                service: "rviz_vnc".to_string(),
                endpoint: endpoint.to_string(),
                port,
                status: "error".to_string(),
                ownership: "session_managed".to_string(),
                launch_attempted: false,
                launch_command: None,
                message: Some(
                    "rviz_vnc launch command is not configured; set RT_RVIZ_VNC_LAUNCH_CMD or install bundled launcher dependencies (bash/Xvfb/x11vnc/rviz2)"
                        .to_string(),
                ),
                checked_at_unix: unix_now(),
            },
            handle: None,
        };
    };

    let mut cmd = Command::new("bash");
    cmd.args(["-lc", &command]);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return EnsuredBridge {
                status: BridgeServiceState {
                    service: "rviz_vnc".to_string(),
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

    for _ in 0..24 {
        if is_local_port_reachable(port).await {
            return EnsuredBridge {
                status: BridgeServiceState {
                    service: "rviz_vnc".to_string(),
                    endpoint: endpoint.to_string(),
                    port,
                    status: "running".to_string(),
                    ownership: "session_managed".to_string(),
                    launch_attempted: true,
                    launch_command: Some(command.clone()),
                    message: Some("rviz_vnc chain launched by visual_debug session".to_string()),
                    checked_at_unix: unix_now(),
                },
                handle: Some(BridgeProcessHandle { child }),
            };
        }

        match child.try_wait() {
            Ok(Some(exit_status)) => {
                return EnsuredBridge {
                    status: BridgeServiceState {
                        service: "rviz_vnc".to_string(),
                        endpoint: endpoint.to_string(),
                        port,
                        status: "error".to_string(),
                        ownership: "session_managed".to_string(),
                        launch_attempted: true,
                        launch_command: Some(command),
                        message: Some(format!("rviz_vnc chain exited early with {}", exit_status)),
                        checked_at_unix: unix_now(),
                    },
                    handle: None,
                };
            }
            Ok(None) => {}
            Err(err) => {
                return EnsuredBridge {
                    status: BridgeServiceState {
                        service: "rviz_vnc".to_string(),
                        endpoint: endpoint.to_string(),
                        port,
                        status: "error".to_string(),
                        ownership: "session_managed".to_string(),
                        launch_attempted: true,
                        launch_command: Some(command),
                        message: Some(format!("rviz_vnc chain check failed: {}", err)),
                        checked_at_unix: unix_now(),
                    },
                    handle: None,
                };
            }
        }

        sleep(Duration::from_millis(250)).await;
    }

    let _ = child.start_kill();
    let _ = timeout(Duration::from_secs(1), child.wait()).await;
    EnsuredBridge {
        status: BridgeServiceState {
            service: "rviz_vnc".to_string(),
            endpoint: endpoint.to_string(),
            port,
            status: "error".to_string(),
            ownership: "session_managed".to_string(),
            launch_attempted: true,
            launch_command: Some(command),
            message: Some("rviz_vnc chain did not become reachable in time".to_string()),
            checked_at_unix: unix_now(),
        },
        handle: None,
    }
}

fn build_rviz_vnc_launch_command(vnc_port: u16) -> Option<String> {
    let custom = std::env::var("RT_RVIZ_VNC_LAUNCH_CMD")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty());
    if let Some(raw) = custom {
        let rendered = raw
            .replace("{port}", &vnc_port.to_string())
            .replace("{{port}}", &vnc_port.to_string());
        return Some(format!("exec {}", rendered));
    }

    let script = locate_rviz_vnc_launcher_script()?;
    Some(format!(
        "exec bash '{}' --vnc-port {}",
        script.to_string_lossy().replace('\'', "'\"'\"'"),
        vnc_port
    ))
}

fn locate_rviz_vnc_launcher_script() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("RT_RVIZ_VNC_SCRIPT_PATH") {
        let candidate = PathBuf::from(raw.trim());
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    let cwd_candidate = PathBuf::from("scripts/rviz_vnc_session.sh");
    if cwd_candidate.is_file() {
        return Some(cwd_candidate);
    }

    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.parent().map(Path::to_path_buf).unwrap_or_default();
        p.push("scripts");
        p.push("rviz_vnc_session.sh");
        if p.is_file() {
            return Some(p);
        }
    }

    materialize_embedded_rviz_vnc_launcher().ok()
}

fn materialize_embedded_rviz_vnc_launcher() -> Result<PathBuf, String> {
    let mut target = std::env::temp_dir();
    target.push("rt_rviz_vnc_session.sh");

    match fs::read_to_string(&target) {
        Ok(existing) if existing == EMBEDDED_RVIZ_VNC_LAUNCHER => return Ok(target),
        Ok(_) => {}
        Err(_) => {}
    }

    fs::write(&target, EMBEDDED_RVIZ_VNC_LAUNCHER).map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(0o755);
        let _ = fs::set_permissions(&target, perms);
    }
    Ok(target)
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
    use super::{build_rviz_vnc_launch_command, parse_endpoint_port};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn parse_endpoint_port_handles_ws_urls() {
        assert_eq!(parse_endpoint_port("ws://localhost:8765"), Some(8765));
        assert_eq!(
            parse_endpoint_port("ws://127.0.0.1:9090/some/path"),
            Some(9090)
        );
        assert_eq!(parse_endpoint_port("vnc://localhost:5901"), Some(5901));
    }

    #[test]
    fn rviz_launch_command_expands_custom_port_token() {
        unsafe {
            std::env::set_var(
                "RT_RVIZ_VNC_LAUNCH_CMD",
                "my-launcher --listen-port {port} --flag",
            );
        }
        let command = build_rviz_vnc_launch_command(6010).expect("command");
        assert!(command.contains("my-launcher --listen-port 6010 --flag"));
        unsafe {
            std::env::remove_var("RT_RVIZ_VNC_LAUNCH_CMD");
        }
    }

    #[test]
    fn rviz_launch_command_uses_script_path_override() {
        let mut script = std::env::temp_dir();
        script.push("rt_test_rviz_vnc.sh");
        fs::write(&script, "#!/usr/bin/env bash\necho ok\n").expect("write script");
        #[cfg(unix)]
        {
            let perms = fs::Permissions::from_mode(0o755);
            let _ = fs::set_permissions(&script, perms);
        }

        unsafe {
            std::env::remove_var("RT_RVIZ_VNC_LAUNCH_CMD");
            std::env::set_var(
                "RT_RVIZ_VNC_SCRIPT_PATH",
                script.to_string_lossy().to_string(),
            );
        }
        let command = build_rviz_vnc_launch_command(6123).expect("command");
        assert!(command.contains("6123"));
        assert!(command.contains("rt_test_rviz_vnc.sh"));

        unsafe {
            std::env::remove_var("RT_RVIZ_VNC_SCRIPT_PATH");
        }
        let _ = fs::remove_file(script);
    }
}
