//! Application layer for the RoboTunnel agent.
//!
//! This module owns robot-facing capabilities:
//! - built-in skill registration
//! - monitor background service
//! - request dispatch into the application router

mod contracts;
mod monitor_config;

use self::contracts::BuiltinContracts;
pub use self::monitor_config::MonitorSettings;
use rt_agent_dispatch::router::Router;
use rt_agent_dispatch::Skill;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use rt_core::tunnel::IncomingCommand;
use rt_llm::Provider;
use rt_skill_acceptance::{AcceptanceReport, RobotObservation};
use rt_skill_fleet::{FleetCompareReport, RobotTelemetry};
use rt_skill_monitor::{MetricSnapshot, MonitorConfig, MonitorService};
use rt_skill_ros2::Ros2Skill;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub fn build_application_router(broadcast_tx: broadcast::Sender<Vec<u8>>) -> Router {
    let mut router = Router::new(broadcast_tx);
    let contracts = Arc::new(BuiltinContracts::new());

    let system_contracts = contracts.clone();
    router.register("system", move |req, _tx| {
        let contracts = system_contracts.clone();
        async move { handle_system_skill(req, &contracts).await }
    });
    tracing::info!("registered skill: system");

    let debug_contracts = contracts.clone();
    router.register("debug", move |req, tx| {
        let contracts = debug_contracts.clone();
        async move {
            if let Err(err) = contracts.validate(&req) {
                return err_response(req.id, err);
            }
            rt_skill_debug::handle(req, tx).await
        }
    });
    tracing::info!("registered skill: debug");

    let ros2_skill = Arc::new(Ros2Skill::new("ws://localhost:9090"));
    let ros2_contracts = contracts.clone();
    router.register("ros2", move |req, tx| {
        let contracts = ros2_contracts.clone();
        let skill = ros2_skill.clone();
        async move {
            if let Err(err) = contracts.validate(&req) {
                return err_response(req.id, err);
            }
            let action = req.action.clone();
            let params = req.params.clone();
            let id = req.id.clone();
            match skill.execute(&action, params, tx).await {
                Ok(data) => CommandResponse {
                    id,
                    status: CommandStatus::Ok,
                    data: Some(data),
                    error: None,
                },
                Err(e) => CommandResponse {
                    id,
                    status: CommandStatus::Error,
                    data: None,
                    error: Some(e.to_string()),
                },
            }
        }
    });
    tracing::info!("registered skill: ros2 (target: ws://localhost:9090)");

    let monitor_contracts = contracts.clone();
    router.register("monitor", move |req, _tx| {
        let contracts = monitor_contracts.clone();
        async move {
            if let Err(err) = contracts.validate(&req) {
                return err_response(req.id, err);
            }
            handle_monitor_skill(req).await
        }
    });
    tracing::info!("registered skill: monitor");

    let fleet_contracts = contracts.clone();
    router.register("fleet", move |req, _tx| {
        let contracts = fleet_contracts.clone();
        async move {
            if let Err(err) = contracts.validate(&req) {
                return err_response(req.id, err);
            }
            handle_fleet_skill(req).await
        }
    });
    tracing::info!("registered skill: fleet");

    let acceptance_contracts = contracts;
    router.register("acceptance", move |req, _tx| {
        let contracts = acceptance_contracts.clone();
        async move {
            if let Err(err) = contracts.validate(&req) {
                return err_response(req.id, err);
            }
            handle_acceptance_skill(req).await
        }
    });
    tracing::info!("registered skill: acceptance");

    router
}

pub fn start_monitor_service_if_enabled(
    shutdown_rx: watch::Receiver<bool>,
    platform_api_url: Option<String>,
    robot_api_key: Option<String>,
) -> Option<JoinHandle<()>> {
    let local_config = MonitorSettings::load().unwrap_or_else(|err| {
        tracing::warn!("failed to load monitor config: {}", err);
        MonitorSettings::default()
    });

    let monitor_enabled = match std::env::var("RT_MONITOR_ENABLED") {
        Ok(v) => parse_bool_env_value(&v),
        Err(_) => local_config.enabled,
    };
    if !monitor_enabled {
        tracing::info!("monitor background service disabled (RT_MONITOR_ENABLED=false)");
        return None;
    }

    let provider = std::env::var("RT_MONITOR_PROVIDER")
        .ok()
        .or_else(|| local_config.provider.clone())
        .and_then(|s| Provider::from_str(&s).ok());
    let robot_id = std::env::var("RT_ROBOT_ID")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    let interval = std::env::var("RT_MONITOR_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(local_config.sample_interval_secs);
    let webhook = std::env::var("RT_MONITOR_WEBHOOK_URL")
        .ok()
        .or_else(|| local_config.webhook_url.clone());
    let notify_target = std::env::var("RT_MONITOR_NOTIFY")
        .ok()
        .unwrap_or_else(|| local_config.notify.clone());
    let cpu_threshold = std::env::var("RT_MONITOR_CPU_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .or(local_config.cpu_threshold_percent);
    let mem_threshold = std::env::var("RT_MONITOR_MEM_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .or(local_config.mem_threshold_percent);

    let svc = MonitorService::new(MonitorConfig {
        sample_interval_secs: interval,
        alert_webhook_url: webhook,
        platform_api_url,
        robot_api_key,
        notify_target,
        llm_provider: provider,
        robot_id,
        cpu_threshold_percent: cpu_threshold,
        mem_threshold_percent: mem_threshold,
        ..MonitorConfig::default()
    });

    Some(tokio::spawn(async move {
        svc.run(shutdown_rx).await;
    }))
}

async fn handle_system_skill(req: CommandRequest, contracts: &BuiltinContracts) -> CommandResponse {
    if let Err(err) = contracts.validate(&req) {
        return err_response(req.id, err);
    }

    match req.action.as_str() {
        "capabilities" => ok_response(req.id, contracts.capabilities_payload()),
        "config_get" => handle_system_config_get(req),
        "config_set" => handle_system_config_set(req),
        _ => err_response(req.id, format!("unknown system action: {}", req.action)),
    }
}

fn handle_system_config_get(req: CommandRequest) -> CommandResponse {
    let Some(section) = req.params.get("section").and_then(|value| value.as_str()) else {
        return err_response(req.id, "missing required param 'section'".to_string());
    };

    match section {
        "monitor" => match MonitorSettings::load() {
            Ok(settings) => match serde_json::to_value(settings) {
                Ok(data) => ok_response(
                    req.id,
                    serde_json::json!({
                        "section": "monitor",
                        "settings": data,
                    }),
                ),
                Err(err) => {
                    err_response(req.id, format!("serialize monitor config failed: {}", err))
                }
            },
            Err(err) => err_response(req.id, format!("load monitor config failed: {}", err)),
        },
        other => err_response(req.id, format!("unknown config section: {}", other)),
    }
}

fn handle_system_config_set(req: CommandRequest) -> CommandResponse {
    let Some(section) = req.params.get("section").and_then(|value| value.as_str()) else {
        return err_response(req.id, "missing required param 'section'".to_string());
    };
    let Some(settings) = req
        .params
        .get("settings")
        .and_then(|value| value.as_object())
    else {
        return err_response(req.id, "missing required param 'settings'".to_string());
    };

    match section {
        "monitor" => {
            let mut current = match MonitorSettings::load() {
                Ok(cfg) => cfg,
                Err(err) => {
                    return err_response(req.id, format!("load monitor config failed: {}", err))
                }
            };
            let warnings = match current.apply_structured_settings(settings) {
                Ok(warnings) => warnings,
                Err(err) => {
                    return err_response(req.id, format!("invalid monitor config: {}", err))
                }
            };
            if let Err(err) = current.save() {
                return err_response(req.id, format!("save monitor config failed: {}", err));
            }
            match serde_json::to_value(&current) {
                Ok(data) => ok_response(
                    req.id,
                    serde_json::json!({
                        "section": "monitor",
                        "settings": data,
                        "warnings": warnings,
                    }),
                ),
                Err(err) => {
                    err_response(req.id, format!("serialize monitor config failed: {}", err))
                }
            }
        }
        other => err_response(req.id, format!("unknown config section: {}", other)),
    }
}

pub async fn dispatch_request(
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

async fn handle_monitor_skill(req: CommandRequest) -> CommandResponse {
    match req.action.as_str() {
        "snapshot" | "status" => match MetricSnapshot::collect() {
            Ok(snap) => ok_response(
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
            Err(e) => err_response(req.id, format!("collect metrics failed: {}", e)),
        },
        _ => err_response(req.id, format!("unknown monitor action: {}", req.action)),
    }
}

async fn handle_fleet_skill(req: CommandRequest) -> CommandResponse {
    if req.action != "compare" {
        return err_response(req.id, format!("unknown fleet action: {}", req.action));
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
        Err(e) => return err_response(req.id, format!("invalid provider: {}", e)),
    };

    let fleet_value = req
        .params
        .get("fleet")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let fleet: Vec<RobotTelemetry> = match serde_json::from_value(fleet_value) {
        Ok(v) => v,
        Err(e) => return err_response(req.id, format!("invalid fleet payload: {}", e)),
    };

    match rt_skill_fleet::compare(query, fleet, &provider).await {
        Ok(report) => fleet_report_response(req.id, report),
        Err(e) => err_response(req.id, format!("fleet compare failed: {}", e)),
    }
}

async fn handle_acceptance_skill(req: CommandRequest) -> CommandResponse {
    if req.action != "run" && req.action != "test" {
        return err_response(req.id, format!("unknown acceptance action: {}", req.action));
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
        Err(e) => return err_response(req.id, format!("invalid provider: {}", e)),
    };

    let observations_value = req
        .params
        .get("observations")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let observations: Vec<RobotObservation> = match serde_json::from_value(observations_value) {
        Ok(v) => v,
        Err(e) => return err_response(req.id, format!("invalid observations payload: {}", e)),
    };

    match rt_skill_acceptance::run_acceptance_test(task, observations, &provider).await {
        Ok(report) => acceptance_report_response(req.id, report),
        Err(e) => err_response(req.id, format!("acceptance test failed: {}", e)),
    }
}

fn ok_response(id: String, data: serde_json::Value) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Ok,
        data: Some(data),
        error: None,
    }
}

fn err_response(id: String, msg: String) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Error,
        data: None,
        error: Some(msg),
    }
}

fn fleet_report_response(id: String, report: FleetCompareReport) -> CommandResponse {
    match serde_json::to_value(report) {
        Ok(v) => ok_response(id, v),
        Err(e) => err_response(id, format!("serialize fleet report failed: {}", e)),
    }
}

fn acceptance_report_response(id: String, report: AcceptanceReport) -> CommandResponse {
    match serde_json::to_value(report) {
        Ok(v) => ok_response(id, v),
        Err(e) => err_response(id, format!("serialize acceptance report failed: {}", e)),
    }
}

fn parse_bool_env_value(v: &str) -> bool {
    matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on")
}
