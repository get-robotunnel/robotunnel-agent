//! RoboTunnel Agent — main entry point.
//!
//! Supports two modes:
//!   1. Agent mode (default): starts the tunnel server, heartbeat, and skill router.
//!   2. Keys mode: manage local encrypted LLM API keys.
//!      robotunnel-agent keys set <provider> <api-key>
//!      robotunnel-agent keys list
//!      robotunnel-agent keys remove <provider>

use bytes::Bytes;
use clap::{Parser, Subcommand};
use rt_core::config::AgentConfig;
use rt_core::heartbeat::HeartbeatService;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus, FrameType};
use rt_core::tunnel::{IncomingCommand, TunnelServer};
use rt_llm::{LlmManager, Provider};
use rt_runtime::router::Router;
use rt_runtime::Skill;
use rt_skill_acceptance::{AcceptanceReport, RobotObservation};
use rt_skill_fleet::{FleetCompareReport, RobotTelemetry};
use rt_skill_monitor::{MetricSnapshot, MonitorConfig, MonitorService};
use rt_skill_ros2::Ros2Skill;
use rt_webrtc::{ConnectionType, WebRtcConfig};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep, Duration};
use tracing;
use tracing_subscriber::{fmt, EnvFilter};
use webrtc::data_channel::RTCDataChannel;

#[derive(Parser, Debug)]
#[command(name = "robotunnel-agent")]
#[command(version = "0.3.0")]
#[command(about = "RoboTunnel Agent — The Physical World API Layer")]
struct Args {
    /// Path to config file
    #[arg(
        short,
        long,
        default_value = "/etc/robotunnel/agent.toml",
        global = true
    )]
    config: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Manage local encrypted LLM API keys
    Keys {
        #[command(subcommand)]
        action: KeysAction,
    },
}

#[derive(Subcommand, Debug)]
enum KeysAction {
    /// Store an API key for an LLM provider (encrypted locally)
    Set {
        /// Provider name: openai, claude, gemini, grok, deepseek, minimax, kimi, qwen
        provider: String,
        /// Your API key (stored encrypted on this machine only — never sent to our servers)
        api_key: String,
    },
    /// List configured LLM providers and their masked keys
    List,
    /// Remove an API key
    Remove {
        /// Provider name to remove
        provider: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Handle key management subcommands (no tunnel needed)
    if let Some(Command::Keys { action }) = args.command {
        return handle_keys(action).await;
    }

    // --- Agent mode ---

    // Load configuration
    let config = if std::path::Path::new(&args.config).exists() {
        AgentConfig::load_with_env(&args.config)?
    } else {
        tracing::info!("config file not found at {}, using defaults", args.config);
        let mut config = AgentConfig::default();
        config.apply_env_overrides();
        config
    };

    // Initialize logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));
    fmt().with_env_filter(filter).with_target(false).init();

    tracing::info!("robotunnel-agent v0.3.0 starting");

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Command channel: tunnel -> router
    let (cmd_tx, cmd_rx) = mpsc::channel(256);

    if config.server.authorized_keys.is_empty() {
        tracing::warn!(
            "server.authorized_keys is empty: agent will accept any valid Ed25519 signature (development mode)"
        );
    } else {
        tracing::info!(
            "server authorized key allowlist enabled ({} key(s))",
            config.server.authorized_keys.len()
        );
    }

    // Build tunnel server
    let webrtc_cmd_tx = cmd_tx.clone();
    let tunnel_server = TunnelServer::new(
        config.server.listen_port,
        config.server.authorized_keys.clone(),
        cmd_tx,
    );

    // Build skill router
    let mut router = Router::new(tunnel_server.broadcast_tx());

    // Register debug skill
    router.register("debug", |req, tx| async move {
        rt_skill_debug::handle(req, tx).await
    });
    tracing::info!("registered skill: debug");

    // Register ROS2 skill
    let ros2_skill = Arc::new(Ros2Skill::new("ws://localhost:9090"));
    router.register("ros2", move |req, tx| {
        let skill = ros2_skill.clone();
        async move {
            let action = req.action.clone();
            let params = req.params.clone();
            let id = req.id.clone();
            match skill.execute(&action, params, tx).await {
                Ok(data) => rt_core::protocol::CommandResponse {
                    id,
                    status: rt_core::protocol::CommandStatus::Ok,
                    data: Some(data),
                    error: None,
                },
                Err(e) => rt_core::protocol::CommandResponse {
                    id,
                    status: rt_core::protocol::CommandStatus::Error,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }
    });
    tracing::info!("registered skill: ros2 (target: ws://localhost:9090)");

    // Register monitor skill (remote status + proactive monitor)
    router.register("monitor", |req, _tx| async move {
        handle_monitor_skill(req).await
    });
    tracing::info!("registered skill: monitor");

    // Register fleet skill (batch comparison)
    router.register(
        "fleet",
        |req, _tx| async move { handle_fleet_skill(req).await },
    );
    tracing::info!("registered skill: fleet");

    // Register acceptance skill (non-technical acceptance testing)
    router.register("acceptance", |req, _tx| async move {
        handle_acceptance_skill(req).await
    });
    tracing::info!("registered skill: acceptance");

    // Optional proactive monitor service in background.
    let monitor_handle = if parse_bool_env("RT_MONITOR_ENABLED", true) {
        let provider = std::env::var("RT_MONITOR_PROVIDER")
            .ok()
            .and_then(|s| Provider::from_str(&s).ok());
        let robot_id = std::env::var("RT_ROBOT_ID")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "unknown".to_string());
        let interval = std::env::var("RT_MONITOR_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let webhook = std::env::var("RT_MONITOR_WEBHOOK_URL").ok();

        let svc = MonitorService::new(MonitorConfig {
            sample_interval_secs: interval,
            alert_webhook_url: webhook,
            llm_provider: provider,
            robot_id,
            ..MonitorConfig::default()
        });

        let shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            svc.run(shutdown_rx).await;
        }))
    } else {
        tracing::info!("monitor background service disabled (RT_MONITOR_ENABLED=false)");
        None
    };

    // Heartbeat service
    let heartbeat_handle = if let Some(api_key) = &config.platform.api_key {
        let heartbeat = HeartbeatService::new(
            config.platform.api_url.clone(),
            api_key.clone(),
            config.heartbeat.interval_secs,
        );
        let shutdown_rx = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            heartbeat.run(shutdown_rx).await;
        }))
    } else {
        tracing::warn!("no API key configured, heartbeat disabled");
        None
    };

    // Optional WebRTC bootstrap service (agent role).
    let webrtc_handle =
        start_webrtc_service_if_enabled(&config, webrtc_cmd_tx, shutdown_rx.clone());

    // Spawn router
    let router_handle = tokio::spawn(async move {
        router.run(cmd_rx).await;
    });

    // Ctrl-C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("shutdown signal received");
        let _ = shutdown_tx_clone.send(true);
    });

    tracing::info!(
        "agent ready — listening on 0.0.0.0:{}",
        config.server.listen_port
    );
    if let Err(e) = tunnel_server.run().await {
        tracing::error!("tunnel server error: {}", e);
    }

    let _ = shutdown_tx.send(true);
    tunnel_server.shutdown();
    if let Some(handle) = heartbeat_handle {
        handle.abort();
    }
    if let Some(handle) = monitor_handle {
        handle.abort();
    }
    if let Some(handle) = webrtc_handle {
        handle.abort();
    }
    router_handle.abort();

    tracing::info!("agent shutdown complete");
    Ok(())
}

/// Handle `robotunnel-agent keys ...` subcommands.
async fn handle_keys(action: KeysAction) -> anyhow::Result<()> {
    match action {
        KeysAction::Set { provider, api_key } => {
            let p = Provider::from_str(&provider)?;
            let mut mgr = LlmManager::open()?;
            mgr.set_key(&p, &api_key)?;
            println!(
                "✓ API key set for {} — stored encrypted on this device only.",
                p.display_name()
            );
        }
        KeysAction::List => {
            let mgr = LlmManager::open()?;
            let keys = mgr.list_keys();
            if keys.is_empty() {
                println!("No LLM API keys configured.\n");
                println!("Add one with: robotunnel-agent keys set <provider> <api-key>");
                println!("Providers: openai, claude, gemini, grok, deepseek, minimax, kimi, qwen");
            } else {
                println!("{:<20} {:<15}", "Provider", "API Key (masked)");
                println!("{}", "-".repeat(36));
                for (provider, masked) in keys {
                    println!("{:<20} {:<15}", provider.display_name(), masked);
                }
                println!(
                    "\nNote: Keys are encrypted with AES-256-GCM using your machine's hardware ID."
                );
                println!("They never leave this device.");
            }
        }
        KeysAction::Remove { provider } => {
            let p = Provider::from_str(&provider)?;
            let mut mgr = LlmManager::open()?;
            if mgr.remove_key(&p)? {
                println!("✓ API key removed for {}.", p.display_name());
            } else {
                println!("No key was set for {}.", p.display_name());
            }
        }
    }
    Ok(())
}

async fn handle_monitor_skill(req: CommandRequest) -> CommandResponse {
    match req.action.as_str() {
        "snapshot" | "status" => match MetricSnapshot::collect() {
            Ok(snap) => ok_resp(
                req.id,
                serde_json::json!({
                    "timestamp_unix": snap.timestamp_unix,
                    "cpu_percent": snap.cpu_percent,
                    "mem_used_mb": snap.mem_used_mb,
                    "mem_total_mb": snap.mem_total_mb,
                    "mem_percent": snap.mem_percent(),
                    "disk_used_gb": snap.disk_used_gb,
                    "disk_total_gb": snap.disk_total_gb,
                    "ros_node_count": snap.ros_node_count,
                }),
            ),
            Err(e) => err_resp(req.id, format!("collect metrics failed: {}", e)),
        },
        _ => err_resp(req.id, format!("unknown monitor action: {}", req.action)),
    }
}

async fn handle_fleet_skill(req: CommandRequest) -> CommandResponse {
    if req.action != "compare" {
        return err_resp(req.id, format!("unknown fleet action: {}", req.action));
    }

    let query = req
        .params
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("Compare fleet outliers");
    let provider = req
        .params
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or("openai");
    let provider = match Provider::from_str(provider) {
        Ok(p) => p,
        Err(e) => return err_resp(req.id, format!("invalid provider: {}", e)),
    };

    let fleet_value = req
        .params
        .get("fleet")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let fleet: Vec<RobotTelemetry> = match serde_json::from_value(fleet_value) {
        Ok(v) => v,
        Err(e) => return err_resp(req.id, format!("invalid fleet payload: {}", e)),
    };

    match rt_skill_fleet::compare(query, fleet, &provider).await {
        Ok(report) => fleet_report_resp(req.id, report),
        Err(e) => err_resp(req.id, format!("fleet compare failed: {}", e)),
    }
}

async fn handle_acceptance_skill(req: CommandRequest) -> CommandResponse {
    if req.action != "run" && req.action != "test" {
        return err_resp(req.id, format!("unknown acceptance action: {}", req.action));
    }

    let task = req
        .params
        .get("task")
        .and_then(|v| v.as_str())
        .unwrap_or("Validate robot task readiness");
    let provider = req
        .params
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or("openai");
    let provider = match Provider::from_str(provider) {
        Ok(p) => p,
        Err(e) => return err_resp(req.id, format!("invalid provider: {}", e)),
    };

    let observations_value = req
        .params
        .get("observations")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let observations: Vec<RobotObservation> = match serde_json::from_value(observations_value) {
        Ok(v) => v,
        Err(e) => return err_resp(req.id, format!("invalid observations payload: {}", e)),
    };

    match rt_skill_acceptance::run_acceptance_test(task, observations, &provider).await {
        Ok(report) => acceptance_report_resp(req.id, report),
        Err(e) => err_resp(req.id, format!("acceptance test failed: {}", e)),
    }
}

fn ok_resp(id: String, data: serde_json::Value) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Ok,
        data: Some(data),
        error: None,
    }
}

fn err_resp(id: String, msg: String) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Error,
        data: None,
        error: Some(msg),
    }
}

fn fleet_report_resp(id: String, report: FleetCompareReport) -> CommandResponse {
    match serde_json::to_value(report) {
        Ok(v) => ok_resp(id, v),
        Err(e) => err_resp(id, format!("serialize fleet report failed: {}", e)),
    }
}

fn acceptance_report_resp(id: String, report: AcceptanceReport) -> CommandResponse {
    match serde_json::to_value(report) {
        Ok(v) => ok_resp(id, v),
        Err(e) => err_resp(id, format!("serialize acceptance report failed: {}", e)),
    }
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

fn start_webrtc_service_if_enabled(
    config: &AgentConfig,
    command_tx: mpsc::Sender<IncomingCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !config.webrtc.enabled {
        tracing::info!("webrtc service disabled by config");
        return None;
    }

    let api_key = match config.platform.api_key.clone() {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            tracing::warn!(
                "webrtc enabled but platform.api_key is missing; skipping webrtc service"
            );
            return None;
        }
    };

    let robot_id = config
        .webrtc
        .robot_id
        .clone()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_string());

    let cfg = WebRtcConfig {
        platform_url: to_ws_base_url(&config.platform.api_url),
        robot_id,
        api_key,
        stun_timeout_secs: config.webrtc.stun_timeout_secs,
    };

    Some(tokio::spawn(async move {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let (dc_payload_tx, mut dc_payload_rx) = mpsc::channel::<Vec<u8>>(256);
            tracing::info!("webrtc: attempting bootstrap for robot_id={}", cfg.robot_id);
            let on_message = Arc::new(move |payload: Vec<u8>| {
                let _ = dc_payload_tx.try_send(payload);
            });

            match rt_webrtc::client::connect(&cfg, on_message).await {
                Ok((dc, conn_type)) => {
                    log_webrtc_connected(&conn_type);
                    let (dc_closed_tx, mut dc_closed_rx) = mpsc::channel::<()>(1);
                    dc.on_close(Box::new(move || {
                        let _ = dc_closed_tx.try_send(());
                        Box::pin(async {})
                    }));

                    loop {
                        tokio::select! {
                            Some(payload) = dc_payload_rx.recv() => {
                                handle_webrtc_payload(payload, &command_tx, &dc).await;
                            }
                            Some(_) = dc_closed_rx.recv() => {
                                tracing::warn!("webrtc datachannel closed; will reconnect");
                                break;
                            }
                            _ = shutdown_rx.changed() => {
                                break;
                            }
                        }
                    }

                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "webrtc bootstrap failed (fallback tcp remains active): {}",
                        e
                    );
                    tokio::select! {
                        _ = sleep(Duration::from_secs(20)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                }
            }
        }
    }))
}

fn log_webrtc_connected(conn_type: &ConnectionType) {
    tracing::info!("webrtc connected via {}", conn_type.display());
}

fn to_ws_base_url(api_url: &str) -> String {
    let trimmed = api_url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://") {
        return format!("wss://{}", rest);
    }
    if let Some(rest) = trimmed.strip_prefix("http://") {
        return format!("ws://{}", rest);
    }
    trimmed.to_string()
}

async fn handle_webrtc_payload(
    payload: Vec<u8>,
    command_tx: &mpsc::Sender<IncomingCommand>,
    dc: &Arc<RTCDataChannel>,
) {
    if payload.is_empty() {
        return;
    }

    if let Some((frame_type, frame_data)) = parse_v2_frame(&payload) {
        match frame_type {
            FrameType::CommandRequest => {
                let request = match serde_json::from_slice::<CommandRequest>(frame_data) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("webrtc: invalid framed CommandRequest: {}", e);
                        let resp = CommandResponse {
                            id: "invalid".to_string(),
                            status: CommandStatus::Error,
                            data: None,
                            error: Some(format!("invalid request: {}", e)),
                        };
                        let _ = send_framed_response(dc, FrameType::CommandResponse, &resp).await;
                        return;
                    }
                };
                let response = dispatch_command(command_tx, request).await;
                let _ = send_framed_response(dc, FrameType::CommandResponse, &response).await;
            }
            FrameType::Ping => {
                let pong = encode_v2_frame(FrameType::Pong, &[]);
                let bytes = Bytes::from(pong);
                if let Err(e) = dc.send(&bytes).await {
                    tracing::warn!("webrtc: send framed pong failed: {}", e);
                }
            }
            _ => {
                tracing::debug!("webrtc: ignoring framed type {:?}", frame_type);
            }
        }
        return;
    }

    let request = match serde_json::from_slice::<CommandRequest>(&payload) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("webrtc: invalid json CommandRequest: {}", e);
            let resp = CommandResponse {
                id: "invalid".to_string(),
                status: CommandStatus::Error,
                data: None,
                error: Some(format!("invalid request: {}", e)),
            };
            let _ = send_json_response(dc, &resp).await;
            return;
        }
    };

    let response = dispatch_command(command_tx, request).await;
    let _ = send_json_response(dc, &response).await;
}

async fn dispatch_command(
    command_tx: &mpsc::Sender<IncomingCommand>,
    request: CommandRequest,
) -> CommandResponse {
    let request_id = request.id.clone();
    let (resp_tx, mut resp_rx) = mpsc::channel(1);
    let incoming = IncomingCommand {
        request,
        response_tx: resp_tx,
    };

    if command_tx.send(incoming).await.is_err() {
        return CommandResponse {
            id: request_id,
            status: CommandStatus::Error,
            data: None,
            error: Some("router unavailable".to_string()),
        };
    }

    match tokio::time::timeout(Duration::from_secs(30), resp_rx.recv()).await {
        Ok(Some(resp)) => resp,
        Ok(None) => CommandResponse {
            id: request_id,
            status: CommandStatus::Error,
            data: None,
            error: Some("response channel closed".to_string()),
        },
        Err(_) => CommandResponse {
            id: request_id,
            status: CommandStatus::Timeout,
            data: None,
            error: Some("command timed out".to_string()),
        },
    }
}

async fn send_json_response(
    dc: &Arc<RTCDataChannel>,
    response: &CommandResponse,
) -> anyhow::Result<()> {
    let text = serde_json::to_string(response)?;
    dc.send_text(text).await?;
    Ok(())
}

async fn send_framed_response(
    dc: &Arc<RTCDataChannel>,
    frame_type: FrameType,
    response: &CommandResponse,
) -> anyhow::Result<()> {
    let data = serde_json::to_vec(response)?;
    let frame = encode_v2_frame(frame_type, &data);
    dc.send(&Bytes::from(frame)).await?;
    Ok(())
}

fn parse_v2_frame(buf: &[u8]) -> Option<(FrameType, &[u8])> {
    if buf.len() < 5 {
        return None;
    }
    let frame_type = FrameType::try_from(buf[0]).ok()?;
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if len != buf.len().saturating_sub(5) {
        return None;
    }
    Some((frame_type, &buf[5..]))
}

fn encode_v2_frame(frame_type: FrameType, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(frame_type as u8);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}
