//! Interaction layer for RoboTunnel agent command ingress.
//!
//! This module translates transport payloads into application requests while
//! keeping the transport-specific framing isolated from business handlers.

use crate::application;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rt_core::authorized_keys::fetch_agent_bootstrap;
use rt_core::config::AgentConfig;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus, FrameType};
use rt_core::tunnel::IncomingCommand;
use rt_webrtc::{ConnectionType, WebRtcConfig};
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue, Message};
use webrtc::data_channel::RTCDataChannel;

pub fn start_webrtc_bridge_if_enabled(
    config: &AgentConfig,
    command_tx: mpsc::Sender<IncomingCommand>,
    mut webrtc_trigger_rx: mpsc::Receiver<rt_core::protocol::WebRtcBootstrapPayload>,
    mut webrtc_teardown_rx: mpsc::Receiver<rt_core::protocol::WebRtcTeardownPayload>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
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

    let configured_robot_id = config
        .webrtc
        .robot_id
        .clone()
        .or_else(|| std::env::var("RT_ROBOT_ID").ok())
        .and_then(|value| normalize_robot_id(&value));
    let platform_api_url = config.platform.api_url.clone();
    let platform_ws_url = to_ws_base_url(&platform_api_url);
    let stun_timeout_secs = config.webrtc.stun_timeout_secs;

    Some(tokio::spawn(async move {
        let mut resolved_robot_id = configured_robot_id.clone();
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if resolved_robot_id.is_none() {
                resolved_robot_id =
                    resolve_robot_id_from_platform(&platform_api_url, &api_key).await;
            }

            // On-demand: wait for trigger from platform over TCP tunnel
            let payload = tokio::select! {
                Some(p) = webrtc_trigger_rx.recv() => {
                    tracing::info!("[BOOTSTRAP:{}] webrtc: received trigger signal, starting bootstrap", p.bootstrap_id);
                    p
                }
                _ = shutdown_rx.changed() => { break; }
            };

            let bootstrap_id = payload.bootstrap_id.clone();
            // TODO: Use payload.cli_public_ip / cli_lan_cidr for routing decisions (LAN bypass)
            if let (Some(cli_ip), Some(cli_cidr)) = (&payload.cli_public_ip, &payload.cli_lan_cidr)
            {
                tracing::info!(
                    "[BOOTSTRAP:{}] CLI Info: PublicIP={}, LAN={}",
                    bootstrap_id,
                    cli_ip,
                    cli_cidr
                );
            }

            let robot_id = match resolved_robot_id.clone() {
                Some(value) => value,
                None => {
                    tracing::warn!(
                        "webrtc: robot_id missing and bootstrap lookup failed; retrying later"
                    );
                    tokio::select! {
                        _ = sleep(Duration::from_secs(20)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };

            let (dc_payload_tx, mut dc_payload_rx) = mpsc::channel::<Vec<u8>>(256);
            let on_message = Arc::new(move |payload: Vec<u8>| {
                let _ = dc_payload_tx.try_send(payload);
            });

            // Reconnection loop (allows one retry)
            let mut attempts = 0;
            const MAX_RETRY: usize = 1;

            loop {
                let cfg = WebRtcConfig {
                    platform_url: platform_ws_url.clone(),
                    robot_id: robot_id.clone(),
                    api_key: api_key.clone(),
                    stun_timeout_secs,
                    bootstrap_id: Some(bootstrap_id.clone()),
                };

                tracing::info!(
                    "[BOOTSTRAP:{}] webrtc: attempting bootstrap (attempt {})",
                    bootstrap_id,
                    attempts + 1
                );

                match rt_webrtc::client::connect(&cfg, on_message.clone()).await {
                    Ok((dc, conn_type)) => {
                        log_webrtc_connected(&conn_type);

                        let (dc_closed_tx, mut dc_closed_rx) = mpsc::channel::<()>(1);
                        dc.on_close(Box::new(move || {
                            let _ = dc_closed_tx.try_send(());
                            Box::pin(async {})
                        }));

                        let mut is_teardown = false;
                        loop {
                            tokio::select! {
                                Some(p) = dc_payload_rx.recv() => {
                                    handle_webrtc_payload(p, &command_tx, &dc).await;
                                }
                                Some(_) = dc_closed_rx.recv() => {
                                    tracing::warn!("[BOOTSTRAP:{}] webrtc datachannel closed", bootstrap_id);
                                    break;
                                }
                                Some(teardown) = webrtc_teardown_rx.recv() => {
                                    if teardown_matches(&teardown, &bootstrap_id) {
                                        tracing::info!("[BOOTSTRAP:{}] webrtc: received teardown signal, closing", bootstrap_id);
                                        is_teardown = true;
                                        break;
                                    }
                                    tracing::debug!("[BOOTSTRAP:{}] webrtc: ignoring teardown for different bootstrap_id", bootstrap_id);
                                }
                                _ = shutdown_rx.changed() => {
                                    break;
                                }
                            }
                        }

                        if *shutdown_rx.borrow() || is_teardown {
                            break; // Exit connection loop
                        }

                        // Unexpected close: try auto-reconnect once
                        if attempts < MAX_RETRY {
                            attempts += 1;
                            tracing::info!(
                                "[BOOTSTRAP:{}] connection dropped; attempting auto-reconnect...",
                                bootstrap_id
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "[BOOTSTRAP:{}] webrtc bootstrap failed: {:#}",
                            bootstrap_id,
                            e
                        );
                        if attempts < MAX_RETRY {
                            attempts += 1;
                            tracing::info!(
                                "[BOOTSTRAP:{}] retrying webrtc bootstrap...",
                                bootstrap_id
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        break;
                    }
                }
            }
            tracing::info!(
                "[BOOTSTRAP:{}] ending bootstrap session; waiting for trigger...",
                bootstrap_id
            );
        }
    }))
}

#[derive(Debug, serde::Deserialize)]
struct AgentControlInbound {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(default)]
    bootstrap_id: Option<String>,
    #[serde(default)]
    session_key: Option<String>,
    #[serde(default)]
    cli_public_ip: Option<String>,
    #[serde(default)]
    cli_lan_cidr: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    command: Option<serde_json::Value>,
}

pub fn start_control_plane_bridge_if_enabled(
    config: &AgentConfig,
    command_tx: mpsc::Sender<IncomingCommand>,
    webrtc_trigger_tx: mpsc::Sender<rt_core::protocol::WebRtcBootstrapPayload>,
    webrtc_teardown_tx: mpsc::Sender<rt_core::protocol::WebRtcTeardownPayload>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    let api_key = match config.platform.api_key.clone() {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            tracing::warn!("control plane channel disabled: platform.api_key is missing");
            return None;
        }
    };

    let configured_robot_id = config
        .webrtc
        .robot_id
        .clone()
        .or_else(|| std::env::var("RT_ROBOT_ID").ok())
        .and_then(|value| normalize_robot_id(&value));
    let platform_api_url = config.platform.api_url.clone();
    let platform_ws_url = to_ws_base_url(&platform_api_url);

    Some(tokio::spawn(async move {
        let mut resolved_robot_id = configured_robot_id.clone();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if resolved_robot_id.is_none() {
                resolved_robot_id =
                    resolve_robot_id_from_platform(&platform_api_url, &api_key).await;
            }

            let robot_id = match resolved_robot_id.clone() {
                Some(v) => v,
                None => {
                    tracing::warn!(
                        "control plane channel: robot_id missing and bootstrap lookup failed; retrying"
                    );
                    tokio::select! {
                        _ = sleep(Duration::from_secs(20)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };

            let control_url = format!(
                "{}/api/agent/connect?robot_id={}",
                platform_ws_url, robot_id
            );
            tracing::info!(
                "[control] connecting to platform control channel: robot={} endpoint={}/api/agent/connect",
                robot_id,
                platform_ws_url
            );

            let mut request = match control_url.clone().into_client_request() {
                Ok(req) => req,
                Err(err) => {
                    tracing::warn!(
                        "[control] failed to build ws request: robot={} err={:#}",
                        robot_id,
                        err
                    );
                    tokio::select! {
                        _ = sleep(Duration::from_secs(3)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };
            let header_value = match HeaderValue::from_str(&api_key) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        "[control] invalid api_key header value: robot={} err={:#}",
                        robot_id,
                        err
                    );
                    tokio::select! {
                        _ = sleep(Duration::from_secs(3)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };
            request
                .headers_mut()
                .insert("X-Robot-API-Key", header_value);

            let ws = match tokio_tungstenite::connect_async(request).await {
                Ok((stream, _)) => {
                    tracing::info!("[control] connected: robot={}", robot_id);
                    stream
                }
                Err(err) => {
                    tracing::warn!("[control] connect failed: robot={} err={:#}", robot_id, err);
                    tokio::select! {
                        _ = sleep(Duration::from_secs(3)) => {}
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();
            let mut ping_tick = tokio::time::interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = ping_tick.tick() => {
                        if let Err(err) = write.send(Message::Ping(Vec::new().into())).await {
                            tracing::warn!("[control] ping failed: robot={} err={}", robot_id, err);
                            break;
                        }
                    }
                    maybe_msg = read.next() => {
                        match maybe_msg {
                            Some(Ok(Message::Text(text))) => {
                                match serde_json::from_str::<AgentControlInbound>(&text) {
                                    Ok(msg) => {
                                        match msg.msg_type.as_str() {
                                            "webrtc_bootstrap" => {
                                                let session_key = normalize_session_key(
                                                    msg.session_key.clone(),
                                                    msg.bootstrap_id.clone(),
                                                );
                                                let payload = rt_core::protocol::WebRtcBootstrapPayload {
                                                    bootstrap_id: session_key,
                                                    cli_public_ip: normalize_optional(msg.cli_public_ip),
                                                    cli_lan_cidr: normalize_optional(msg.cli_lan_cidr),
                                                };
                                                match webrtc_trigger_tx.try_send(payload) {
                                                    Ok(()) => {}
                                                    Err(TrySendError::Closed(_)) => {
                                                        tracing::warn!("[control] webrtc trigger channel closed");
                                                        continue;
                                                    }
                                                    Err(TrySendError::Full(_)) => {
                                                        tracing::warn!("[control] webrtc trigger channel full; dropping stale trigger");
                                                        continue;
                                                    }
                                                }
                                            }
                                            "webrtc_teardown" => {
                                                let session_key = normalize_optional_session_key(
                                                    msg.session_key.clone(),
                                                    msg.bootstrap_id.clone(),
                                                );
                                                let payload = rt_core::protocol::WebRtcTeardownPayload {
                                                    bootstrap_id: session_key,
                                                };
                                                match webrtc_teardown_tx.try_send(payload) {
                                                    Ok(()) => {}
                                                    Err(TrySendError::Closed(_)) => {
                                                        tracing::warn!("[control] webrtc teardown channel closed");
                                                        continue;
                                                    }
                                                    Err(TrySendError::Full(_)) => {
                                                        tracing::warn!("[control] webrtc teardown channel full; dropping stale teardown");
                                                        continue;
                                                    }
                                                }
                                            }
                                            "command_request" => {
                                                let session_key = normalize_optional_session_key(
                                                    msg.session_key.clone(),
                                                    msg.bootstrap_id.clone(),
                                                );
                                                let response = match msg.command {
                                                    Some(raw) => match serde_json::from_value::<CommandRequest>(raw) {
                                                        Ok(request) => application::dispatch_request(&command_tx, request).await,
                                                        Err(err) => CommandResponse {
                                                            id: "invalid".to_string(),
                                                            status: CommandStatus::Error,
                                                            data: None,
                                                            error: Some(format!("invalid command payload: {}", err)),
                                                        },
                                                    },
                                                    None => CommandResponse {
                                                        id: "invalid".to_string(),
                                                        status: CommandStatus::Error,
                                                        data: None,
                                                        error: Some("missing command payload".to_string()),
                                                    },
                                                };

                                                let mut outbound = serde_json::Map::new();
                                                outbound.insert(
                                                    "type".to_string(),
                                                    serde_json::Value::String("command_response".to_string()),
                                                );
                                                outbound.insert(
                                                    "request_id".to_string(),
                                                    serde_json::Value::String(normalize_bootstrap_id(msg.request_id)),
                                                );
                                                outbound.insert(
                                                    "response".to_string(),
                                                    serde_json::to_value(response).unwrap_or(serde_json::Value::Null),
                                                );
                                                if let Some(key) = session_key {
                                                    outbound.insert(
                                                        "session_key".to_string(),
                                                        serde_json::Value::String(key.clone()),
                                                    );
                                                    outbound.insert(
                                                        "bootstrap_id".to_string(),
                                                        serde_json::Value::String(key),
                                                    );
                                                }

                                                let outbound = serde_json::Value::Object(outbound).to_string();
                                                if let Err(err) = write.send(Message::Text(outbound.into())).await {
                                                    tracing::warn!("[control] command response write failed: robot={} err={}", robot_id, err);
                                                    break;
                                                }
                                            }
                                            other => {
                                                tracing::debug!("[control] ignoring message type={}", other);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!("[control] invalid control message: {}", err);
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                if let Err(err) = write.send(Message::Pong(payload)).await {
                                    tracing::warn!("[control] pong write failed: robot={} err={}", robot_id, err);
                                    break;
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                tracing::info!("[control] platform closed control channel: robot={}", robot_id);
                                break;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(err)) => {
                                tracing::warn!("[control] read failed: robot={} err={}", robot_id, err);
                                break;
                            }
                            None => {
                                tracing::info!("[control] channel ended: robot={}", robot_id);
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        let _ = write.send(Message::Close(None)).await;
                        return;
                    }
                }
            }

            tokio::select! {
                _ = sleep(Duration::from_secs(2)) => {}
                _ = shutdown_rx.changed() => { break; }
            }
        }
    }))
}

async fn resolve_robot_id_from_platform(api_url: &str, api_key: &str) -> Option<String> {
    match fetch_agent_bootstrap(api_url, api_key).await {
        Ok(bootstrap) => bootstrap.robot_id.as_deref().and_then(normalize_robot_id),
        Err(err) => {
            tracing::warn!(
                "webrtc: failed to resolve robot_id from platform: {:#}",
                err
            );
            None
        }
    }
}

fn normalize_robot_id(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("unknown") {
        return None;
    }
    Some(value.to_string())
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn normalize_bootstrap_id(value: Option<String>) -> String {
    match normalize_optional(value) {
        Some(v) => v,
        None => uuid::Uuid::new_v4().to_string(),
    }
}

fn normalize_session_key(session_key: Option<String>, bootstrap_id: Option<String>) -> String {
    match normalize_optional_session_key(session_key, bootstrap_id) {
        Some(v) => v,
        None => uuid::Uuid::new_v4().to_string(),
    }
}

fn normalize_optional_session_key(
    session_key: Option<String>,
    bootstrap_id: Option<String>,
) -> Option<String> {
    normalize_optional(session_key).or_else(|| normalize_optional(bootstrap_id))
}

fn teardown_matches(payload: &rt_core::protocol::WebRtcTeardownPayload, current_id: &str) -> bool {
    let id = payload
        .bootstrap_id
        .as_ref()
        .and_then(|value| normalize_optional(Some(value.clone())));
    match id {
        Some(v) => v == current_id,
        None => true,
    }
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
                        let resp = invalid_request_response(e);
                        let _ = send_framed_response(dc, FrameType::CommandResponse, &resp).await;
                        return;
                    }
                };
                let response = application::dispatch_request(command_tx, request).await;
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
            let resp = invalid_request_response(e);
            let _ = send_json_response(dc, &resp).await;
            return;
        }
    };

    let response = application::dispatch_request(command_tx, request).await;
    let _ = send_json_response(dc, &response).await;
}

fn invalid_request_response(error: serde_json::Error) -> CommandResponse {
    CommandResponse {
        id: "invalid".to_string(),
        status: CommandStatus::Error,
        data: None,
        error: Some(format!("invalid request: {}", error)),
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
