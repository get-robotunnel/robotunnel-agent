//! Interaction layer for RoboTunnel agent command ingress.
//!
//! This module translates transport payloads into application requests while
//! keeping the transport-specific framing isolated from business handlers.

use crate::application;
use bytes::Bytes;
use rt_core::config::AgentConfig;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus, FrameType};
use rt_core::tunnel::IncomingCommand;
use rt_webrtc::{ConnectionType, WebRtcConfig};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use webrtc::data_channel::RTCDataChannel;

pub fn start_webrtc_bridge_if_enabled(
    config: &AgentConfig,
    command_tx: mpsc::Sender<IncomingCommand>,
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

    let cfg = WebRtcConfig {
        platform_url: to_ws_base_url(&config.platform.api_url),
        robot_id: config
            .webrtc
            .robot_id
            .clone()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| "unknown".to_string()),
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
