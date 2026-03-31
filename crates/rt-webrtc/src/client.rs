//! WebRTC client for the RoboTunnel Agent.
//!
//! Implements a single-pass ICE strategy:
//!   1. Fetch TURN credentials (best effort)
//!   2. Build one ICE config with STUN + TURN candidates
//!   3. Run one parallel ICE attempt within a bounded timeout window

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures::{SinkExt, StreamExt};
// use serde_json::Value; // Removed unused
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};
use tracing::{debug, info, warn};
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors, media_engine::MediaEngine,
        setting_engine::SettingEngine, APIBuilder,
    },
    data_channel::RTCDataChannel,
    dtls::extension::extension_use_srtp::SrtpProtectionProfile,
    ice::network_type::NetworkType,
    ice_transport::{
        ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
        ice_candidate_pair::RTCIceCandidatePair,
        ice_connection_state::RTCIceConnectionState,
        ice_server::RTCIceServer,
    },
    interceptor::registry::Registry,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        policy::ice_transport_policy::RTCIceTransportPolicy,
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
    let relay_required = route_requires_relay(cfg);
    info!(
        "[BOOTSTRAP:{}] WebRTC: preparing parallel ICE (STUN+TURN)...",
        id
    );
    if relay_required {
        info!(
            "[BOOTSTRAP:{}] route_type=turn_relay -> enforcing relay-only ICE policy",
            id
        );
    }
    log_phase(cfg, BootstrapPhase::StunStart, None);

    let mut ice_servers = Vec::new();
    if !relay_required {
        ice_servers.push(RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        });
    }
    let mut turn_enabled = false;

    match fetch_turn_credentials(cfg).await {
        Ok(turn_creds) => {
            if !relay_required {
                for stun_url in &turn_creds.stun_urls {
                    ice_servers.push(RTCIceServer {
                        urls: vec![stun_url.clone()],
                        ..Default::default()
                    });
                }
            }

            if turn_creds.turn_available {
                if let Some(turn) = turn_creds.turn {
                    let filtered_turn_urls = filter_supported_turn_urls(&turn.urls);
                    let tcp_enabled = env_flag_enabled_with_default("RT_WEBRTC_TCP_ENABLED", false);
                    for raw in &turn.urls {
                        if is_supported_turn_url(raw, tcp_enabled) {
                            continue;
                        }
                        match turn_url_transport(raw) {
                            Some(TurnUrlTransport::Tcp) => warn!(
                                "WebRTC: dropping TCP TURN URL because RT_WEBRTC_TCP_ENABLED=false: {}",
                                raw
                            ),
                            _ => warn!(
                                "WebRTC: skipping unsupported TURN URL for current ICE stack: {}",
                                raw
                            ),
                        }
                    }
                    if !filtered_turn_urls.is_empty() {
                        info!(
                            "WebRTC: TURN relay candidates enabled ({})",
                            filtered_turn_urls.join(", ")
                        );
                        ice_servers.push(RTCIceServer {
                            urls: filtered_turn_urls,
                            username: turn.username,
                            credential: turn.credential,
                            ..Default::default()
                        });
                        turn_enabled = true;
                    } else {
                        if relay_required {
                            bail!("relay-only mode requested but no supported TURN URL remained");
                        } else {
                            warn!(
                                "WebRTC: TURN credentials fetched but no supported TURN URL remained; continuing with STUN only"
                            );
                        }
                    }
                } else {
                    if relay_required {
                        bail!("relay-only mode requested but TURN credentials missing");
                    } else {
                        warn!(
                            "WebRTC: TURN marked available but credentials missing; continuing with STUN only"
                        );
                    }
                }
            } else {
                if relay_required {
                    bail!("relay-only mode requested but TURN not available");
                } else {
                    warn!("WebRTC: TURN not available from platform; continuing with STUN only");
                }
            }
        }
        Err(err) => {
            if relay_required {
                return Err(err)
                    .context("relay-only mode requested but TURN credential fetch failed");
            } else {
                warn!(
                    "WebRTC: failed to fetch TURN credentials ({:#}); continuing with STUN only",
                    err
                );
            }
        }
    }

    let dc = attempt_webrtc(cfg, ice_servers, on_message)
        .await
        .context("WebRTC failed during parallel ICE attempt")?;

    if turn_enabled {
        info!(
            "[BOOTSTRAP:{}] WebRTC: connected (parallel ICE with TURN relay enabled)",
            id
        );
        Ok((dc, ConnectionType::Turn))
    } else {
        log_stun_success();
        Ok((dc, ConnectionType::Stun))
    }
}

/// Inner connection attempt with a given set of ICE servers.
async fn attempt_webrtc(
    cfg: &WebRtcConfig,
    ice_servers: Vec<RTCIceServer>,
    on_message: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
) -> Result<Arc<RTCDataChannel>> {
    let cfg_clone = cfg.clone();
    // Build WebRTC API
    let network_types = ice_network_types_for_servers(&ice_servers);
    let mut setting_engine = SettingEngine::default();
    setting_engine.set_network_types(network_types.clone());
    // Avoid negotiating AEAD_AES_256_GCM with current stack versions.
    // webrtc-srtp 0.14 panics when that profile is selected (expects 16-byte key, gets 32).
    setting_engine.set_srtp_protection_profiles(vec![
        SrtpProtectionProfile::Srtp_Aead_Aes_128_Gcm,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_80,
        SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_32,
    ]);
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

    let mut config = RTCConfiguration {
        ice_servers,
        ..Default::default()
    };
    if route_requires_relay(cfg) {
        config.ice_transport_policy = RTCIceTransportPolicy::Relay;
    }

    let pc = Arc::new(api.new_peer_connection(config).await?);

    let (dc_open_tx, dc_open_rx) = tokio::sync::oneshot::channel::<()>();
    let dc_open_tx = Arc::new(std::sync::Mutex::new(Some(dc_open_tx)));

    // DataChannel for bidirectional data transfer
    let dc = pc.create_data_channel("rt-data", None).await?;
    {
        let on_msg = on_message.clone();
        let dc_open_tx = dc_open_tx.clone();
        dc.on_message(Box::new(move |msg| {
            on_msg(msg.data.to_vec());
            Box::pin(async {})
        }));
        dc.on_open(Box::new(move || {
            let dc_open_tx = dc_open_tx.clone();
            Box::pin(async move {
                info!("WebRTC DataChannel opened");
                if let Ok(mut guard) = dc_open_tx.lock() {
                    if let Some(tx) = guard.take() {
                        let _ = tx.send(());
                    }
                }
            })
        }));
    }

    // Log selected ICE pair for protocol-level diagnosis (stun vs turn/udp vs turn/tcp)
    pc.sctp()
        .transport()
        .ice_transport()
        .on_selected_candidate_pair_change(Box::new(|pair: RTCIceCandidatePair| {
            Box::pin(async move {
                info!("WebRTC selected candidate pair: {}", pair);
            })
        }));

    // Connect to signaling server
    let sig_url = cfg_clone.signaling_url();
    debug!("WebRTC: connecting to signaling server: {}", sig_url);
    log_phase(&cfg_clone, BootstrapPhase::SignalWsConnectStart, None);
    let mut ws_req = sig_url.clone().into_client_request().with_context(|| {
        let err_msg = format!("invalid signaling WebSocket URL (url={})", sig_url);
        log_phase(
            &cfg_clone,
            BootstrapPhase::SignalWsConnectFail,
            Some(&err_msg),
        );
        err_msg
    })?;
    let api_key_header = HeaderValue::from_str(&cfg.api_key).with_context(|| {
        let err_msg = "invalid robot API key for signaling header";
        log_phase(
            &cfg_clone,
            BootstrapPhase::SignalWsConnectFail,
            Some(err_msg),
        );
        err_msg
    })?;
    ws_req
        .headers_mut()
        .insert("X-Robot-API-Key", api_key_header);
    let (ws_stream, _) = connect_async(ws_req).await.with_context(|| {
        let err_msg = format!("signaling WebSocket connect failed (url={})", sig_url);
        log_phase(
            &cfg_clone,
            BootstrapPhase::SignalWsConnectFail,
            Some(&err_msg),
        );
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
    let connect_timeout = resolve_parallel_connect_timeout(cfg);
    let (connected_tx, connected_rx) =
        tokio::sync::oneshot::channel::<std::result::Result<(), String>>();
    let connected_tx = Arc::new(std::sync::Mutex::new(Some(connected_tx)));

    {
        let ctx = connected_tx.clone();
        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            let ctx = ctx.clone();
            Box::pin(async move {
                match s {
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

    {
        let ctx = connected_tx.clone();
        let cfg_clone3 = cfg_clone.clone();
        pc.on_ice_connection_state_change(Box::new(move |s: RTCIceConnectionState| {
            let ctx = ctx.clone();
            let cfg_phase = cfg_clone3.clone();
            Box::pin(async move {
                match s {
                    RTCIceConnectionState::Connected | RTCIceConnectionState::Completed => {
                        log_phase(&cfg_phase, BootstrapPhase::StunConnected, None);
                        if let Ok(mut guard) = ctx.lock() {
                            if let Some(tx) = guard.take() {
                                let _ = tx.send(Ok(()));
                            }
                        }
                    }
                    RTCIceConnectionState::Failed => {
                        if let Ok(mut guard) = ctx.lock() {
                            if let Some(tx) = guard.take() {
                                let _ = tx.send(Err(format!("ICE state: {:?}", s)));
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

    // Phase 1: ICE connectivity must be established within the 2+2s window.
    match timeout(connect_timeout, connected_rx).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => bail!("WebRTC connection failed: {}", e),
        Ok(Err(_)) => bail!("Connection state channel dropped"),
        Err(_) => {
            log_phase(&cfg_clone, BootstrapPhase::StunTimeout, None);
            bail!("WebRTC ICE timeout ({}s)", connect_timeout.as_secs());
        }
    }

    // Phase 2: DataChannel must become usable shortly after ICE connected.
    let dc_open_grace = resolve_datachannel_open_grace();
    match timeout(dc_open_grace, dc_open_rx).await {
        Ok(Ok(())) => {
            log_phase(cfg, BootstrapPhase::DataChannelOpen, None);
            Ok(dc)
        }
        Ok(Err(_)) => bail!("DataChannel open signal dropped"),
        Err(_) => bail!(
            "WebRTC DataChannel open timeout ({}s) after ICE connected",
            dc_open_grace.as_secs()
        ),
    }
}

/// Fetch TURN credentials from the platform.
async fn fetch_turn_credentials(cfg: &WebRtcConfig) -> Result<TurnCredentialResponse> {
    log_phase(cfg, BootstrapPhase::TurnFetchStart, None);
    let client = reqwest::Client::new();
    let url = cfg.turn_credentials_url();
    let resp = client
        .get(&url)
        .header("X-Robot-API-Key", cfg.api_key.clone())
        .send()
        .await
        .with_context(|| {
            let err_msg = format!(
                "HTTP transport error fetching TURN credentials from {}",
                url
            );
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

fn route_requires_relay(cfg: &WebRtcConfig) -> bool {
    if let Some(route_type) = cfg.route_type.as_ref() {
        if route_type.trim().eq_ignore_ascii_case("turn_relay") {
            return true;
        }
    }
    env_flag_enabled_with_default("RT_WEBRTC_FORCE_RELAY", false)
}

fn resolve_parallel_connect_timeout(cfg: &WebRtcConfig) -> Duration {
    const FIRST_WINDOW_SECS: u64 = 2;
    const EXTRA_WINDOW_SECS: u64 = 2;
    const TOTAL_WINDOW_SECS: u64 = FIRST_WINDOW_SECS + EXTRA_WINDOW_SECS;

    let mut secs = cfg.stun_timeout_secs;
    if let Ok(raw) = std::env::var("RT_WEBRTC_CONNECT_TIMEOUT_SECS") {
        if let Ok(parsed) = raw.trim().parse::<u64>() {
            secs = parsed;
        }
    }
    if secs == 0 {
        secs = TOTAL_WINDOW_SECS;
    }
    if secs < FIRST_WINDOW_SECS {
        secs = FIRST_WINDOW_SECS;
    }
    if secs > TOTAL_WINDOW_SECS {
        secs = TOTAL_WINDOW_SECS;
    }
    Duration::from_secs(secs)
}

fn resolve_datachannel_open_grace() -> Duration {
    const DEFAULT_GRACE_SECS: u64 = 8;
    const MAX_GRACE_SECS: u64 = 12;

    let mut secs = DEFAULT_GRACE_SECS;
    if let Ok(raw) = std::env::var("RT_WEBRTC_DC_OPEN_GRACE_SECS") {
        if let Ok(parsed) = raw.trim().parse::<u64>() {
            secs = parsed;
        }
    }
    if secs == 0 {
        secs = DEFAULT_GRACE_SECS;
    }
    if secs > MAX_GRACE_SECS {
        secs = MAX_GRACE_SECS;
    }
    Duration::from_secs(secs)
}

fn ice_network_types_for_servers(ice_servers: &[RTCIceServer]) -> Vec<NetworkType> {
    let ipv6_enabled = env_flag_enabled("RT_WEBRTC_IPV6_ENABLED");
    let tcp_enabled = env_flag_enabled_with_default("RT_WEBRTC_TCP_ENABLED", false);
    let needs_tcp = tcp_enabled
        && ice_servers
            .iter()
            .flat_map(|server| server.urls.iter())
            .any(|url| turn_url_transport(url) == Some(TurnUrlTransport::Tcp));

    let mut out = vec![NetworkType::Udp4];
    if ipv6_enabled {
        out.push(NetworkType::Udp6);
    }
    if needs_tcp {
        out.push(NetworkType::Tcp4);
        if ipv6_enabled {
            out.push(NetworkType::Tcp6);
        }
    }
    out
}

fn env_flag_enabled(name: &str) -> bool {
    env_flag_enabled_with_default(name, false)
}

fn env_flag_enabled_with_default(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|value| parse_bool_like(&value))
        .unwrap_or(default)
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
    let tcp_enabled = env_flag_enabled_with_default("RT_WEBRTC_TCP_ENABLED", false);
    let tls_only = tcp_enabled && env_flag_enabled_with_default("RT_WEBRTC_TURN_TLS_ONLY", false);
    let mut expanded: Vec<(String, bool)> = Vec::new();
    for raw in urls {
        let turns_origin = raw.trim().to_ascii_lowercase().starts_with("turns:");
        for expanded_url in expand_turn_urls(raw, tcp_enabled) {
            let is_tls_tcp =
                turns_origin && turn_url_transport(&expanded_url) == Some(TurnUrlTransport::Tcp);
            expanded.push((expanded_url, is_tls_tcp));
        }
    }

    let mut out = Vec::new();
    if tls_only {
        for (url, is_tls_tcp) in &expanded {
            if !is_tls_tcp {
                continue;
            }
            if !out.iter().any(|existing| existing == url) {
                out.push(url.clone());
            }
        }
        if !out.is_empty() {
            info!(
                "WebRTC: RT_WEBRTC_TURN_TLS_ONLY active; using TLS TURN candidates only ({})",
                out.join(", ")
            );
            return out;
        }
        warn!(
            "WebRTC: RT_WEBRTC_TURN_TLS_ONLY active but no TLS TURN candidate found; falling back to full supported TURN candidate set"
        );
    }

    for (url, _) in expanded {
        if !out.iter().any(|existing| existing == &url) {
            out.push(url);
        }
    }
    out
}

#[cfg(test)]
fn clear_turn_env_for_test() {
    std::env::remove_var("RT_WEBRTC_TCP_ENABLED");
    std::env::remove_var("RT_WEBRTC_TURN_TLS_ONLY");
}

#[cfg(test)]
fn set_env_for_test(name: &str, value: &str) {
    std::env::set_var(name, value);
}

#[cfg(test)]
fn normalize_urls(mut urls: Vec<String>) -> Vec<String> {
    urls.sort();
    urls
}

#[cfg(test)]
mod turn_filter_tests {
    use super::*;

    #[test]
    fn test_filter_supported_turn_urls_default_keeps_all_supported_candidates() {
        clear_turn_env_for_test();
        set_env_for_test("RT_WEBRTC_TCP_ENABLED", "1");
        set_env_for_test("RT_WEBRTC_TURN_TLS_ONLY", "0");
        let urls = vec![
            "turn:turn.robotunnel.io:3478".to_string(),
            "turns:turn.robotunnel.io:5349".to_string(),
        ];
        let got = normalize_urls(filter_supported_turn_urls(&urls));
        let want = normalize_urls(vec![
            "turn:turn.robotunnel.io:3478?transport=udp".to_string(),
            "turn:turn.robotunnel.io:3478?transport=tcp".to_string(),
            "turn:turn.robotunnel.io:5349?transport=tcp".to_string(),
        ]);
        assert_eq!(got, want);
        clear_turn_env_for_test();
    }

    #[test]
    fn test_filter_supported_turn_urls_tls_only_can_be_enabled() {
        clear_turn_env_for_test();
        set_env_for_test("RT_WEBRTC_TCP_ENABLED", "1");
        set_env_for_test("RT_WEBRTC_TURN_TLS_ONLY", "1");
        let urls = vec![
            "turn:turn.robotunnel.io:3478".to_string(),
            "turns:turn.robotunnel.io:5349".to_string(),
        ];
        let got = filter_supported_turn_urls(&urls);
        assert_eq!(got, vec!["turn:turn.robotunnel.io:5349?transport=tcp"]);
        clear_turn_env_for_test();
    }
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
        assert!(is_supported_turn_url("turn:turn.robotunnel.io:3478", true));
        assert!(is_supported_turn_url(
            "turn:turn.robotunnel.io:3478?transport=udp",
            true
        ));
        assert!(is_supported_turn_url(
            "turn:turn.robotunnel.io:3478?transport=tcp",
            true
        ));
        assert!(is_supported_turn_url("turns:turn.robotunnel.io:5349", true));
        assert!(!is_supported_turn_url(
            "turns:turn.robotunnel.io:5349?transport=udp",
            true
        ));
        assert!(!is_supported_turn_url(
            "turn:turn.robotunnel.io:3478?transport=tcp",
            false
        ));
        assert!(!is_supported_turn_url(
            "turns:turn.robotunnel.io:5349",
            false
        ));
    }

    #[test]
    fn test_ice_network_types_include_tcp_when_turn_tcp_present() {
        std::env::set_var("RT_WEBRTC_TCP_ENABLED", "1");
        let servers = vec![RTCIceServer {
            urls: vec!["turn:turn.robotunnel.io:3478?transport=tcp".to_string()],
            ..Default::default()
        }];
        let types = ice_network_types_for_servers(&servers);
        assert!(types.contains(&NetworkType::Udp4));
        assert!(types.contains(&NetworkType::Tcp4));
        std::env::remove_var("RT_WEBRTC_TCP_ENABLED");
    }

    #[test]
    fn test_expand_turn_urls_normalizes_turns_and_adds_tcp_variant() {
        let urls = expand_turn_urls("turns:turn.robotunnel.io:5349", true);
        assert_eq!(urls, vec!["turn:turn.robotunnel.io:5349?transport=tcp"]);

        let urls = expand_turn_urls("turn:turn.robotunnel.io:3478", true);
        assert_eq!(
            urls,
            vec![
                "turn:turn.robotunnel.io:3478?transport=udp",
                "turn:turn.robotunnel.io:3478?transport=tcp"
            ]
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnUrlTransport {
    Udp,
    Tcp,
}

fn is_supported_turn_url(url: &str, tcp_enabled: bool) -> bool {
    match turn_url_transport(url) {
        Some(TurnUrlTransport::Udp) => true,
        Some(TurnUrlTransport::Tcp) => tcp_enabled,
        None => false,
    }
}

fn expand_turn_urls(url: &str, tcp_enabled: bool) -> Vec<String> {
    let normalized = url.trim().to_ascii_lowercase();
    let (base, query) = normalized
        .split_once('?')
        .map(|(lhs, rhs)| (lhs, Some(rhs)))
        .unwrap_or((normalized.as_str(), None));
    let query_transport = query.and_then(parse_turn_transport_query);

    if base.starts_with("turns:") {
        if !tcp_enabled || query_transport == Some("udp") {
            return Vec::new();
        }
        let host = base.trim_start_matches("turns:");
        return vec![format!("turn:{}?transport=tcp", host)];
    }

    if !base.starts_with("turn:") {
        return Vec::new();
    }

    let host = base.trim_start_matches("turn:");
    match query_transport {
        Some("udp") => vec![format!("turn:{}?transport=udp", host)],
        Some("tcp") if tcp_enabled => vec![format!("turn:{}?transport=tcp", host)],
        Some("tcp") => Vec::new(),
        Some(_) => Vec::new(),
        None if tcp_enabled => vec![
            format!("turn:{}?transport=udp", host),
            format!("turn:{}?transport=tcp", host),
        ],
        None => vec![format!("turn:{}?transport=udp", host)],
    }
}

fn turn_url_transport(url: &str) -> Option<TurnUrlTransport> {
    let normalized = url.trim().to_ascii_lowercase();
    let (base, query) = normalized
        .split_once('?')
        .map(|(lhs, rhs)| (lhs, Some(rhs)))
        .unwrap_or((normalized.as_str(), None));

    let query_transport = query.and_then(parse_turn_transport_query);

    if base.starts_with("turns:") {
        return match query_transport {
            Some("udp") => None,
            Some("tcp") | None => Some(TurnUrlTransport::Tcp),
            Some(_) => None,
        };
    }

    if !base.starts_with("turn:") {
        return None;
    }

    match query_transport {
        Some("udp") | None => Some(TurnUrlTransport::Udp),
        Some("tcp") => Some(TurnUrlTransport::Tcp),
        Some(_) => None,
    }
}

fn parse_turn_transport_query(query: &str) -> Option<&str> {
    query.split('&').find_map(|kv| {
        let (key, value) = kv.split_once('=')?;
        if key == "transport" {
            Some(value)
        } else {
            None
        }
    })
}
