//! WebRTC client for the RoboTunnel Agent.
//!
//! Implements the three-phase connection strategy:
//!   1. Try STUN-only ICE (timeout: `stun_timeout_secs`)
//!   2. If STUN fails, fetch TURN credentials and retry
//!   3. If both fail, signal TcpTunnel fallback to caller

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
// use serde_json::Value; // Removed unused
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors, media_engine::MediaEngine,
        setting_engine::SettingEngine, APIBuilder,
    },
    data_channel::RTCDataChannel,
    ice::network_type::NetworkType,
    ice_transport::{
        ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
        ice_server::RTCIceServer,
    },
    interceptor::registry::Registry,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
};

use crate::{types::*, ConnectionType, WebRtcConfig};

/// Establishes a WebRTC DataChannel connection to a CLI peer.
///
/// Returns the connection type used (STUN direct or TURN relay) and
/// the established DataChannel for bidirectional data transfer.
///
/// On error, the caller should fall back to TCP tunnel.
pub async fn connect(
    cfg: &WebRtcConfig,
    on_message: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
) -> Result<(Arc<RTCDataChannel>, ConnectionType)> {
    let id = cfg.bootstrap_id.as_deref().unwrap_or("none");
    // Phase 1: attempt STUN-only
    info!("[BOOTSTRAP:{}] WebRTC: attempting STUN (direct P2P)...", id);
    log_phase(cfg, BootstrapPhase::StunStart, None);
    let stun_ice_servers = vec![RTCIceServer {
        urls: vec!["stun:stun.l.google.com:19302".to_string()],
        ..Default::default()
    }];

    match attempt_webrtc(cfg, stun_ice_servers, on_message.clone()).await {
        Ok(dc) => {
            log_stun_success();
            return Ok((dc, ConnectionType::Stun));
        }
        Err(e) => {
            log_phase(cfg, BootstrapPhase::StunTimeout, Some(&e.to_string()));
            warn!(
                "WebRTC bootstrap before direct STUN completion failed: {:#}. Fetching TURN credentials...",
                e
            );
        }
    }

    // Phase 2: fetch TURN credentials and retry
    let turn_creds = fetch_turn_credentials(cfg).await?;
    if !turn_creds.turn_available {
        bail!("TURN not available on platform — WebRTC cannot proceed");
    }

    let turn = turn_creds
        .turn
        .context("TURN credentials missing from response")?;
    let filtered_turn_urls = filter_supported_turn_urls(&turn.urls);
    for skipped in turn
        .urls
        .iter()
        .filter(|url| !filtered_turn_urls.iter().any(|kept| kept == *url))
    {
        warn!(
            "WebRTC: skipping unsupported TURN URL for current ICE stack: {}",
            skipped
        );
    }
    if filtered_turn_urls.is_empty() {
        bail!(
            "TURN credentials available but no supported TURN URL (currently requires turn:... over UDP)"
        );
    }

    let mut ice_servers = turn_creds
        .stun_urls
        .iter()
        .map(|u| RTCIceServer {
            urls: vec![u.clone()],
            ..Default::default()
        })
        .collect::<Vec<_>>();

    ice_servers.push(RTCIceServer {
        urls: filtered_turn_urls.clone(),
        username: turn.username.clone(),
        credential: turn.credential.clone(),
        ..Default::default()
    });

    info!(
        "WebRTC: retrying with TURN relay ({})...",
        filtered_turn_urls.join(", ")
    );
    let dc = attempt_webrtc(cfg, ice_servers, on_message)
        .await
        .context("WebRTC failed with both STUN and TURN")?;

    info!("[BOOTSTRAP:{}] WebRTC: connected via TURN relay", id);
    Ok((dc, ConnectionType::Turn))
}

/// Inner connection attempt with a given set of ICE servers.
async fn attempt_webrtc(
    cfg: &WebRtcConfig,
    ice_servers: Vec<RTCIceServer>,
    on_message: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
) -> Result<Arc<RTCDataChannel>> {
    let cfg_clone = cfg.clone();
    // Build WebRTC API
    let network_types = ice_network_types_from_env();
    let mut setting_engine = SettingEngine::default();
    setting_engine.set_network_types(network_types.clone());
    info!(
        "WebRTC: ICE network types = {}",
        render_network_types(&network_types)
    );

    let mut m = MediaEngine::default();
    m.register_default_codecs()?;
    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)?;
    let api = APIBuilder::new()
        .with_setting_engine(setting_engine)
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();

    let config = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };

    let pc = Arc::new(api.new_peer_connection(config).await?);

    // DataChannel for bidirectional data transfer
    let dc = pc.create_data_channel("rt-data", None).await?;
    {
        let on_msg = on_message.clone();
        dc.on_message(Box::new(move |msg| {
            on_msg(msg.data.to_vec());
            Box::pin(async {})
        }));
        dc.on_open(Box::new(|| {
            info!("WebRTC DataChannel opened");
            Box::pin(async {})
        }));
    }

    // Connect to signaling server
    let sig_url = cfg_clone.signaling_url();
    debug!("WebRTC: connecting to signaling server: {}", sig_url);
    log_phase(&cfg_clone, BootstrapPhase::SignalWsConnectStart, None);
    let (ws_stream, _) = connect_async(&sig_url)
        .await
        .with_context(|| {
            let err_msg = format!("signaling WebSocket connect failed (url={})", sig_url);
            log_phase(&cfg_clone, BootstrapPhase::SignalWsConnectFail, Some(&err_msg));
            err_msg
        })?;
    log_phase(&cfg_clone, BootstrapPhase::SignalWsConnectOk, None);
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Collect local ICE candidates to send after offer
    let (ice_tx, mut ice_rx) = tokio::sync::mpsc::channel::<RTCIceCandidateInit>(32);
    {
        let ice_tx = ice_tx.clone();
        pc.on_ice_candidate(Box::new(move |c: Option<RTCIceCandidate>| {
            let ice_tx = ice_tx.clone();
            Box::pin(async move {
                if let Some(candidate) = c {
                    if let Ok(init) = candidate.to_json() {
                        let _ = ice_tx.send(init).await;
                    }
                }
            })
        }));
    }

    // Create SDP offer
    let offer = pc.create_offer(None).await?;
    pc.set_local_description(offer.clone()).await?;

    let offer_msg = SignalMessage {
        r#type: "offer".to_string(),
        payload: Some(serde_json::to_value(&offer)?),
        robot_id: cfg.robot_id.clone(),
        bootstrap_id: cfg.bootstrap_id.clone(),
    };
    ws_tx
        .send(Message::Text(serde_json::to_string(&offer_msg)?.into()))
        .await?;
    debug!("WebRTC: sent SDP offer");
    log_phase(&cfg_clone, BootstrapPhase::OfferSent, None);

    // Wait for answer + ICE candidates (with timeout)
    let connect_timeout = Duration::from_secs(cfg.stun_timeout_secs.max(10));
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let connected_tx = std::sync::Mutex::new(Some(connected_tx));

    {
        let ctx = Arc::new(connected_tx);
        let cfg_clone2 = cfg_clone.clone();
        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            let ctx = ctx.clone();
            let cfg_phase = cfg_clone2.clone();
            Box::pin(async move {
                match s {
                    RTCPeerConnectionState::Connected => {
                        log_phase(&cfg_phase, BootstrapPhase::StunConnected, None);
                        if let Ok(mut guard) = ctx.lock() {
                            if let Some(tx) = guard.take() {
                                let _ = tx.send(Ok(()));
                            }
                        }
                    }
                    RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected => {
                        if let Ok(mut guard) = ctx.lock() {
                            if let Some(tx) = guard.take() {
                                let _ = tx.send(Err(format!("Connection state: {:?}", s)));
                            }
                        }
                    }
                    _ => {}
                }
            })
        }));
    }

    // Signal exchange loop in background
    let pc_clone = pc.clone();
    let cfg_signaling = cfg_clone.clone();
    tokio::spawn(async move {
        // Forward local ICE candidates to remote
        loop {
            tokio::select! {
                Some(candidate) = ice_rx.recv() => {
                    let msg = SignalMessage {
                        r#type: "ice-candidate".to_string(),
                        payload: Some(serde_json::to_value(&candidate).unwrap_or(serde_json::Value::Null)),
                        robot_id: cfg_signaling.robot_id.clone(),
                        bootstrap_id: cfg_signaling.bootstrap_id.clone(),
                    };
                    if let Ok(json) = serde_json::to_string(&msg) {
                        let _ = ws_tx.send(Message::Text(json.into())).await;
                    }
                }
                Some(Ok(ws_msg)) = ws_rx.next() => {
                    if let Message::Text(txt) = ws_msg {
                        if let Ok(sig) = serde_json::from_str::<SignalMessage>(&txt) {
                            match sig.r#type.as_str() {
                                "answer" => {
                                    if let Some(payload) = sig.payload {
                                        if let Ok(answer) = serde_json::from_value::<RTCSessionDescription>(payload) {
                                            let _ = pc_clone.set_remote_description(answer).await;
                                            debug!("WebRTC: remote answer set");
                                            log_phase(&cfg_signaling, BootstrapPhase::AnswerReceived, None);
                                        }
                                    }
                                }
                                "ice-candidate" => {
                                    if let Some(payload) = sig.payload {
                                        if let Ok(init) = serde_json::from_value::<RTCIceCandidateInit>(payload) {
                                            let _ = pc_clone.add_ice_candidate(init).await;
                                        }
                                    }
                                }
                                "bye" => break,
                                _ => {}
                            }
                        }
                    }
                }
                else => break,
            }
        }
    });

    // Wait for connection with timeout
    match timeout(connect_timeout, connected_rx).await {
        Ok(Ok(Ok(()))) => {
            log_phase(cfg, BootstrapPhase::DataChannelOpen, None);
            Ok(dc)
        }
        Ok(Ok(Err(e))) => bail!("WebRTC connection failed: {}", e),
        Ok(Err(_)) => bail!("Connection state channel dropped"),
        Err(_) => {
            log_phase(&cfg_clone, BootstrapPhase::StunTimeout, None);
            bail!("WebRTC ICE timeout ({}s)", connect_timeout.as_secs());
        }
    }
}

/// Fetch TURN credentials from the platform.
async fn fetch_turn_credentials(cfg: &WebRtcConfig) -> Result<TurnCredentialResponse> {
    log_phase(cfg, BootstrapPhase::TurnFetchStart, None);
    let client = reqwest::Client::new();
    let url = cfg.turn_credentials_url();
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| {
            let err_msg = format!("HTTP transport error fetching TURN credentials from {}", url);
            log_phase(cfg, BootstrapPhase::TurnFetchFail, Some(&err_msg));
            err_msg
        })?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_else(|_| "no body".to_string());
        let err_msg = format!("TURN credentials API returned {}: {}", status, body);
        log_phase(cfg, BootstrapPhase::TurnFetchFail, Some(&err_msg));
        bail!(err_msg);
    }

    let payload = resp
        .json::<TurnCredentialResponse>()
        .await
        .with_context(|| {
            let err_msg = "failed to parse TURN credentials JSON";
            log_phase(cfg, BootstrapPhase::TurnFetchFail, Some(err_msg));
            err_msg
        })?;

    log_phase(cfg, BootstrapPhase::TurnFetchOk, None);
    Ok(payload)
}

fn log_phase(cfg: &WebRtcConfig, phase: BootstrapPhase, detail: Option<&str>) {
    let id = cfg.bootstrap_id.as_deref().unwrap_or("none");
    if let Some(d) = detail {
        warn!("[BOOTSTRAP:{}] {} - {}", id, phase.as_str(), d);
    } else {
        info!("[BOOTSTRAP:{}] {}", id, phase.as_str());
    }
}

fn log_stun_success() {
    info!("WebRTC: connected via STUN (direct P2P) — no relay bandwidth used");
}

fn ice_network_types_from_env() -> Vec<NetworkType> {
    if env_flag_enabled("RT_WEBRTC_IPV6_ENABLED") {
        return vec![NetworkType::Udp4, NetworkType::Udp6];
    }
    vec![NetworkType::Udp4]
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| parse_bool_like(&value))
        .unwrap_or(false)
}

fn render_network_types(network_types: &[NetworkType]) -> String {
    network_types
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_bool_like(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn filter_supported_turn_urls(urls: &[String]) -> Vec<String> {
    urls.iter()
        .filter(|url| is_supported_turn_url(url))
        .cloned()
        .collect()
}

fn is_supported_turn_url(url: &str) -> bool {
    let normalized = url.trim().to_ascii_lowercase();
    if !normalized.starts_with("turn:") {
        return false;
    }

    let Some((_, query)) = normalized.split_once('?') else {
        return true;
    };

    for kv in query.split('&') {
        if let Some(value) = kv.strip_prefix("transport=") {
            return value == "udp";
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bool_like_true_values() {
        assert!(parse_bool_like("true"));
        assert!(parse_bool_like("1"));
        assert!(parse_bool_like("YES"));
    }

    #[test]
    fn test_parse_bool_like_false_values() {
        assert!(!parse_bool_like("false"));
        assert!(!parse_bool_like("0"));
        assert!(!parse_bool_like("off"));
    }

    #[test]
    fn test_is_supported_turn_url() {
        assert!(is_supported_turn_url("turn:turn.robotunnel.io:3478"));
        assert!(is_supported_turn_url(
            "turn:turn.robotunnel.io:3478?transport=udp"
        ));
        assert!(!is_supported_turn_url(
            "turn:turn.robotunnel.io:3478?transport=tcp"
        ));
        assert!(!is_supported_turn_url("turns:turn.robotunnel.io:5349"));
    }
}
