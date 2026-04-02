//! Interaction layer for RoboTunnel agent command ingress.
//!
//! This module translates transport payloads into application requests while
//! keeping the transport-specific framing isolated from business handlers.

use crate::application;
use base64::Engine as _;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rt_core::authorized_keys::fetch_agent_bootstrap;
use rt_core::config::AgentConfig;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus, FrameType};
use rt_core::tunnel::IncomingCommand;
use rt_webrtc::{ConnectionType, WebRtcConfig};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration, Instant};
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
        let mut pending_trigger: Option<rt_core::protocol::WebRtcBootstrapPayload> = None;
        'bridge: loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if resolved_robot_id.is_none() {
                resolved_robot_id =
                    resolve_robot_id_from_platform(&platform_api_url, &api_key).await;
            }

            // On-demand: wait for trigger from platform over TCP tunnel.
            // If a newer trigger arrives while a session is active, it is stashed
            // into pending_trigger and consumed here first to avoid queue buildup.
            let payload = if let Some(p) = pending_trigger.take() {
                tracing::info!(
                    "[BOOTSTRAP:{}] webrtc: processing queued trigger",
                    p.bootstrap_id
                );
                p
            } else {
                tokio::select! {
                    Some(p) = webrtc_trigger_rx.recv() => {
                        tracing::info!("[BOOTSTRAP:{}] webrtc: received trigger signal, starting bootstrap", p.bootstrap_id);
                        p
                    }
                    _ = shutdown_rx.changed() => { break 'bridge; }
                }
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
            if let Some(route_type) = payload.route_type.as_ref() {
                tracing::info!(
                    "[BOOTSTRAP:{}] route_type hint from platform: {}",
                    bootstrap_id,
                    route_type
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
                        _ = shutdown_rx.changed() => { break 'bridge; }
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
                    route_type: payload.route_type.clone(),
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

                        let (relay_out_tx, mut relay_out_rx) = mpsc::channel::<Vec<u8>>(512);
                        let (relay_closed_tx, mut relay_closed_rx) = mpsc::channel::<String>(128);
                        let relay_ctx = Arc::new(WebRtcRelayContext::new(
                            relay_out_tx.clone(),
                            relay_closed_tx.clone(),
                        ));
                        let dc_writer = dc.clone();
                        let relay_send_task = tokio::spawn(async move {
                            while let Some(frame) = relay_out_rx.recv().await {
                                if let Err(err) = dc_writer.send(&Bytes::from(frame)).await {
                                    tracing::warn!("webrtc relay outbound send failed: {}", err);
                                    break;
                                }
                            }
                        });

                        let mut is_teardown = false;
                        let mut superseded_by_new_trigger = false;
                        loop {
                            tokio::select! {
                                Some(p) = dc_payload_rx.recv() => {
                                    handle_webrtc_payload(p, &command_tx, &dc, relay_ctx.clone()).await;
                                }
                                Some(next_trigger) = webrtc_trigger_rx.recv() => {
                                    let next_id = next_trigger.bootstrap_id.clone();
                                    if next_id == bootstrap_id {
                                        tracing::debug!(
                                            "[BOOTSTRAP:{}] webrtc: duplicate trigger received while active; ignoring",
                                            bootstrap_id
                                        );
                                        continue;
                                    }
                                    tracing::info!(
                                        "[BOOTSTRAP:{}] webrtc: superseded by newer trigger {}; restarting",
                                        bootstrap_id,
                                        next_id
                                    );
                                    pending_trigger = Some(next_trigger);
                                    superseded_by_new_trigger = true;
                                    break;
                                }
                                Some(_) = dc_closed_rx.recv() => {
                                    tracing::warn!("[BOOTSTRAP:{}] webrtc datachannel closed", bootstrap_id);
                                    break;
                                }
                                Some(relay_id) = relay_closed_rx.recv() => {
                                    relay_ctx.handle_local_relay_closed(relay_id).await;
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
                        relay_ctx.shutdown().await;
                        relay_send_task.abort();

                        if *shutdown_rx.borrow() || is_teardown || superseded_by_new_trigger {
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
    route_type: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    command: Option<serde_json::Value>,
    #[serde(default)]
    status_query: Option<serde_json::Value>,
    #[serde(default)]
    relay_id: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

struct RelaySession {
    write_tx: mpsc::Sender<Vec<u8>>,
    close_tx: watch::Sender<bool>,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
}

#[derive(Debug, Clone)]
struct RelayOutboundEnvelope {
    msg_type: String,
    relay_id: String,
    payload: String,
    enqueued_at: Instant,
}

#[derive(Debug, Default, Clone)]
struct RelayQueueStats {
    sent: u64,
    dropped_data: u64,
    max_queue_depth: usize,
    enqueue_wait_samples: u64,
    enqueue_wait_total_ms: u64,
    enqueue_wait_max_ms: u64,
    scheduler_lag_samples: u64,
    scheduler_lag_total_ms: u64,
    scheduler_lag_max_ms: u64,
}

type RelayQueueStatsMap = Arc<Mutex<HashMap<String, RelayQueueStats>>>;

const RELAY_DATA_QUEUE_LIMIT: usize = 256;
const RELAY_CLOSE_GRACE: Duration = Duration::from_millis(250);

#[derive(Debug, Default, Clone, Copy)]
struct RelayQueuePurgeOutcome {
    dropped_control: usize,
    dropped_data: usize,
}

#[derive(Default)]
struct RelayOutboundQueue {
    control: VecDeque<RelayOutboundEnvelope>,
    data: VecDeque<RelayOutboundEnvelope>,
}

struct RelayOutboundScheduler {
    queues: HashMap<String, RelayOutboundQueue>,
    order: VecDeque<String>,
    data_limit: usize,
}

struct RelayEnqueueOutcome {
    queue_depth: usize,
    dropped_data: bool,
}

impl RelayOutboundScheduler {
    fn new(data_limit: usize) -> Self {
        Self {
            queues: HashMap::new(),
            order: VecDeque::new(),
            data_limit: data_limit.max(1),
        }
    }

    fn enqueue(&mut self, msg: RelayOutboundEnvelope) -> RelayEnqueueOutcome {
        let relay_id = msg.relay_id.trim().to_string();
        let is_data = msg.msg_type.trim() == "relay_data";
        let queue = self.queues.entry(relay_id.clone()).or_default();
        let mut dropped_data = false;
        if is_data {
            if queue.data.len() >= self.data_limit {
                let _ = queue.data.pop_front();
                dropped_data = true;
            }
            queue.data.push_back(msg);
        } else {
            queue.control.push_back(msg);
        }
        if !self.order.iter().any(|item| item == &relay_id) {
            self.order.push_back(relay_id);
        }
        RelayEnqueueOutcome {
            queue_depth: queue.control.len() + queue.data.len(),
            dropped_data,
        }
    }

    fn pop_next(&mut self) -> Option<RelayOutboundEnvelope> {
        // Global control-first scheduling: scan all active relays and send one
        // control frame before any relay_data frame. This keeps relay_open_ack /
        // relay_close responsive even under heavy data bursts from other relays.
        let control_turns = self.order.len();
        for _ in 0..control_turns {
            let relay_id = match self.order.pop_front() {
                Some(v) => v,
                None => return None,
            };
            let (msg, has_more) = match self.queues.get_mut(&relay_id) {
                Some(queue) => {
                    let msg = queue.control.pop_front();
                    let has_more = !queue.control.is_empty() || !queue.data.is_empty();
                    (msg, has_more)
                }
                None => (None, false),
            };
            if has_more {
                self.order.push_back(relay_id.clone());
            } else {
                self.queues.remove(&relay_id);
            }
            if msg.is_some() {
                return msg;
            }
        }

        let data_turns = self.order.len();
        for _ in 0..data_turns {
            let relay_id = match self.order.pop_front() {
                Some(v) => v,
                None => return None,
            };
            let (msg, has_more) = match self.queues.get_mut(&relay_id) {
                Some(queue) => {
                    let msg = queue.data.pop_front();
                    let has_more = !queue.control.is_empty() || !queue.data.is_empty();
                    (msg, has_more)
                }
                None => (None, false),
            };
            if has_more {
                self.order.push_back(relay_id.clone());
            } else {
                self.queues.remove(&relay_id);
            }
            if msg.is_some() {
                return msg;
            }
        }
        None
    }

    fn purge_relay(&mut self, relay_id: &str) -> RelayQueuePurgeOutcome {
        let relay_id = relay_id.trim();
        if relay_id.is_empty() {
            return RelayQueuePurgeOutcome::default();
        }
        self.order.retain(|item| item != relay_id);
        if let Some(queue) = self.queues.remove(relay_id) {
            return RelayQueuePurgeOutcome {
                dropped_control: queue.control.len(),
                dropped_data: queue.data.len(),
            };
        }
        RelayQueuePurgeOutcome::default()
    }
}

fn duration_to_millis_u64(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u64::MAX as u128 {
        u64::MAX
    } else {
        millis as u64
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct WebRtcRelayFrame {
    #[serde(default)]
    relay_id: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

struct WebRtcRelayContext {
    relays: tokio::sync::Mutex<HashMap<String, RelaySession>>,
    outbound_tx: mpsc::Sender<Vec<u8>>,
    relay_closed_tx: mpsc::Sender<String>,
}

impl WebRtcRelayContext {
    fn new(outbound_tx: mpsc::Sender<Vec<u8>>, relay_closed_tx: mpsc::Sender<String>) -> Self {
        Self {
            relays: tokio::sync::Mutex::new(HashMap::new()),
            outbound_tx,
            relay_closed_tx,
        }
    }

    async fn open_relay(&self, relay_id: String, port: u16) -> Result<(), String> {
        let relay_id = relay_id.trim().to_string();
        if relay_id.is_empty() || port == 0 {
            return Err("invalid relay_open payload".to_string());
        }

        let previous = {
            let mut relays = self.relays.lock().await;
            relays.remove(&relay_id)
        };
        if let Some(prev) = previous {
            stop_relay_session(&relay_id, prev, "webrtc_relay_replaced").await;
        }
        let relay = open_local_webrtc_relay(
            relay_id.clone(),
            port,
            self.outbound_tx.clone(),
            self.relay_closed_tx.clone(),
        )
        .await?;
        let mut relays = self.relays.lock().await;
        relays.insert(relay_id, relay);
        Ok(())
    }

    async fn write_relay(&self, relay_id: &str, chunk: Vec<u8>) -> Result<(), String> {
        let relay_id = relay_id.trim();
        if relay_id.is_empty() {
            return Err("relay_id is required".to_string());
        }
        let write_tx = {
            let relays = self.relays.lock().await;
            relays.get(relay_id).map(|relay| relay.write_tx.clone())
        };

        if let Some(tx) = write_tx {
            if tx.send(chunk).await.is_err() {
                let stale = {
                    let mut relays = self.relays.lock().await;
                    relays.remove(relay_id)
                };
                if let Some(stale) = stale {
                    stop_relay_session(relay_id, stale, "webrtc_relay_write_channel_closed").await;
                }
                return Err("relay write channel closed".to_string());
            }
            return Ok(());
        }
        Err("relay not found".to_string())
    }

    async fn close_relay(&self, relay_id: &str) {
        let relay_id = relay_id.trim();
        if relay_id.is_empty() {
            return;
        }
        let relay = {
            let mut relays = self.relays.lock().await;
            relays.remove(relay_id)
        };
        if let Some(relay) = relay {
            stop_relay_session(relay_id, relay, "webrtc_relay_close_requested").await;
        }
    }

    async fn handle_local_relay_closed(&self, relay_id: String) {
        let relay_id = relay_id.trim().to_string();
        if relay_id.is_empty() {
            return;
        }
        let relay = {
            let mut relays = self.relays.lock().await;
            relays.remove(&relay_id)
        };
        if let Some(relay) = relay {
            stop_relay_session(&relay_id, relay, "webrtc_local_target_closed").await;
        }
        let _ = send_webrtc_relay_event(
            &self.outbound_tx,
            FrameType::RelayClose,
            &WebRtcRelayFrame {
                relay_id,
                error: Some("relay target closed".to_string()),
                ..Default::default()
            },
        )
        .await;
    }

    async fn shutdown(&self) {
        let drained_relays: Vec<(String, RelaySession)> = {
            let mut relays = self.relays.lock().await;
            relays.drain().collect()
        };
        for (relay_id, relay) in drained_relays {
            stop_relay_session(&relay_id, relay, "webrtc_context_shutdown").await;
        }
    }
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
        let robot_id = loop {
            if *shutdown_rx.borrow() {
                return;
            }
            if resolved_robot_id.is_none() {
                resolved_robot_id =
                    resolve_robot_id_from_platform(&platform_api_url, &api_key).await;
            }
            if let Some(id) = resolved_robot_id.clone() {
                break id;
            }
            tracing::warn!(
                "control plane channel: robot_id missing and bootstrap lookup failed; retrying"
            );
            tokio::select! {
                _ = sleep(Duration::from_secs(20)) => {}
                _ = shutdown_rx.changed() => { return; }
            }
        };

        let (relay_outbound_tx, relay_outbound_rx) = mpsc::channel::<RelayOutboundEnvelope>(4096);
        let closed_relays: ClosedRelaySet = Arc::new(Mutex::new(HashSet::new()));
        let relay_stats: RelayQueueStatsMap = Arc::new(Mutex::new(HashMap::new()));
        let relay_task = tokio::spawn(run_relay_plane(
            platform_ws_url.clone(),
            robot_id.clone(),
            api_key.clone(),
            relay_outbound_tx.clone(),
            relay_outbound_rx,
            closed_relays.clone(),
            relay_stats.clone(),
            shutdown_rx.clone(),
        ));

        let header_value = match HeaderValue::from_str(&api_key) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    "[control] invalid api_key header value: robot={} err={:#}",
                    robot_id,
                    err
                );
                return;
            }
        };

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let session_id = uuid::Uuid::new_v4().to_string();
            let control_url = format!(
                "{}/api/agent/connect?robot_id={}&session_id={}",
                platform_ws_url, robot_id, session_id
            );
            tracing::info!(
                "[control] connecting to platform channels: robot={} session_id={} control={}/api/agent/connect relay={}/api/agent/relay",
                robot_id,
                session_id,
                platform_ws_url,
                platform_ws_url
            );

            let mut control_request = match control_url.clone().into_client_request() {
                Ok(req) => req,
                Err(err) => {
                    tracing::warn!(
                        "[control] failed to build control ws request: robot={} err={:#}",
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
            control_request
                .headers_mut()
                .insert("X-Robot-API-Key", header_value.clone());

            let control_ws = match tokio_tungstenite::connect_async(control_request).await {
                Ok((stream, _)) => {
                    tracing::info!(
                        "[control] connected: robot={} session_id={}",
                        robot_id,
                        session_id
                    );
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

            let (mut control_write, mut control_read) = control_ws.split();
            let (control_outbound_tx, mut control_outbound_rx) = mpsc::channel::<String>(512);
            let mut control_ping_tick = tokio::time::interval(Duration::from_secs(30));
            let mut control_heartbeat_tick = tokio::time::interval(Duration::from_secs(20));
            let mut control_watchdog_tick = tokio::time::interval(Duration::from_secs(10));
            let mut last_control_inbound = Instant::now();
            let control_inbound_timeout = Duration::from_secs(75);
            let control_write_timeout = Duration::from_secs(8);
            let reconnect_control = loop {
                tokio::select! {
                    _ = control_ping_tick.tick() => {
                        match timeout(control_write_timeout, control_write.send(Message::Ping(Vec::new().into()))).await {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => {
                                tracing::warn!(
                                    "[control] ping failed: robot={} session_id={} err={}",
                                    robot_id,
                                    session_id,
                                    err
                                );
                                break true;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "[control] ping write timeout: robot={} session_id={} timeout={}s",
                                    robot_id,
                                    session_id,
                                    control_write_timeout.as_secs()
                                );
                                break true;
                            }
                        }
                    }
                    _ = control_heartbeat_tick.tick() => {
                        // Some middleboxes are aggressive on control-frame-only websocket traffic.
                        // Emit a lightweight text heartbeat to keep the path active.
                        match timeout(
                            control_write_timeout,
                            control_write.send(Message::Text("{\"type\":\"control_heartbeat\"}".into())),
                        ).await {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => {
                                tracing::warn!(
                                    "[control] heartbeat write failed: robot={} session_id={} err={}",
                                    robot_id,
                                    session_id,
                                    err
                                );
                                break true;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "[control] heartbeat write timeout: robot={} session_id={} timeout={}s",
                                    robot_id,
                                    session_id,
                                    control_write_timeout.as_secs()
                                );
                                break true;
                            }
                        }
                    }
                    _ = control_watchdog_tick.tick() => {
                        if last_control_inbound.elapsed() > control_inbound_timeout {
                            tracing::warn!(
                                "[control] watchdog timeout: robot={} session_id={} no inbound message for {}s, reconnecting",
                                robot_id,
                                session_id,
                                control_inbound_timeout.as_secs()
                            );
                            break true;
                        }
                    }
                    Some(outbound) = control_outbound_rx.recv() => {
                        match timeout(control_write_timeout, control_write.send(Message::Text(outbound.into()))).await {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => {
                                tracing::warn!(
                                    "[control] outbound write failed: robot={} session_id={} err={}",
                                    robot_id,
                                    session_id,
                                    err
                                );
                                break true;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "[control] outbound write timeout: robot={} session_id={} timeout={}s",
                                    robot_id,
                                    session_id,
                                    control_write_timeout.as_secs()
                                );
                                break true;
                            }
                        }
                    }
                    maybe_msg = control_read.next() => {
                        match maybe_msg {
                            Some(Ok(Message::Text(text))) => {
                                last_control_inbound = Instant::now();
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
                                                    route_type: normalize_optional(msg.route_type),
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
                                                if control_outbound_tx.send(outbound).await.is_err() {
                                                    break true;
                                                }
                                            }
                                            "status_request" => {
                                                let session_key = normalize_optional_session_key(
                                                    msg.session_key.clone(),
                                                    msg.bootstrap_id.clone(),
                                                );
                                                let _query = msg.status_query;
                                                let mut response = serde_json::Map::new();
                                                response.insert(
                                                    "status".to_string(),
                                                    serde_json::Value::String("ok".to_string()),
                                                );
                                                response.insert(
                                                    "data".to_string(),
                                                    application::collect_control_plane_status(),
                                                );

                                                let mut outbound = serde_json::Map::new();
                                                outbound.insert(
                                                    "type".to_string(),
                                                    serde_json::Value::String("status_response".to_string()),
                                                );
                                                outbound.insert(
                                                    "request_id".to_string(),
                                                    serde_json::Value::String(normalize_bootstrap_id(msg.request_id)),
                                                );
                                                outbound.insert(
                                                    "response".to_string(),
                                                    serde_json::Value::Object(response),
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
                                                if control_outbound_tx.send(outbound).await.is_err() {
                                                    break true;
                                                }
                                            }
                                            "relay_open" | "relay_data" | "relay_close" => {
                                                tracing::warn!(
                                                    "[control] relay payload received on control channel; ignored: robot={} session_id={} type={}",
                                                    robot_id,
                                                    session_id,
                                                    msg.msg_type
                                                );
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
                                last_control_inbound = Instant::now();
                                match timeout(
                                    control_write_timeout,
                                    control_write.send(Message::Pong(payload)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(err)) => {
                                        tracing::warn!(
                                            "[control] pong write failed: robot={} session_id={} err={}",
                                            robot_id,
                                            session_id,
                                            err
                                        );
                                        break true;
                                    }
                                    Err(_) => {
                                        tracing::warn!(
                                            "[control] pong write timeout: robot={} session_id={} timeout={}s",
                                            robot_id,
                                            session_id,
                                            control_write_timeout.as_secs()
                                        );
                                        break true;
                                    }
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                last_control_inbound = Instant::now();
                            }
                            Some(Ok(Message::Close(_))) => {
                                tracing::info!(
                                    "[control] platform closed control channel: robot={} session_id={}",
                                    robot_id,
                                    session_id
                                );
                                break true;
                            }
                            Some(Ok(_)) => {}
                            Some(Err(err)) => {
                                tracing::warn!(
                                    "[control] read failed: robot={} session_id={} err={}",
                                    robot_id,
                                    session_id,
                                    err
                                );
                                break true;
                            }
                            None => {
                                tracing::info!(
                                    "[control] channel ended: robot={} session_id={}",
                                    robot_id,
                                    session_id
                                );
                                break true;
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break false;
                    }
                }
            };

            let _ = control_write.send(Message::Close(None)).await;

            if !reconnect_control {
                break;
            }
            tokio::select! {
                _ = sleep(Duration::from_secs(2)) => {}
                _ = shutdown_rx.changed() => { break; }
            }
        }

        drop(relay_outbound_tx);
        let _ = timeout(Duration::from_secs(2), relay_task).await;
    }))
}

async fn run_relay_plane(
    platform_ws_url: String,
    robot_id: String,
    api_key: String,
    relay_outbound_tx: mpsc::Sender<RelayOutboundEnvelope>,
    mut relay_outbound_rx: mpsc::Receiver<RelayOutboundEnvelope>,
    closed_relays: ClosedRelaySet,
    relay_stats: RelayQueueStatsMap,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let relay_write_timeout = Duration::from_secs(8);

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        let relay_session_id = uuid::Uuid::new_v4().to_string();
        let relay_url = format!(
            "{}/api/agent/relay?robot_id={}&session_id={}",
            platform_ws_url, robot_id, relay_session_id
        );

        let header_value = match HeaderValue::from_str(&api_key) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    "[relay] invalid api_key header value: robot={} session_id={} err={:#}",
                    robot_id,
                    relay_session_id,
                    err
                );
                return;
            }
        };
        let mut relay_request = match relay_url.clone().into_client_request() {
            Ok(req) => req,
            Err(err) => {
                tracing::warn!(
                    "[relay] failed to build relay ws request: robot={} session_id={} err={:#}",
                    robot_id,
                    relay_session_id,
                    err
                );
                tokio::select! {
                    _ = sleep(Duration::from_secs(2)) => {}
                    _ = shutdown_rx.changed() => {}
                }
                continue;
            }
        };
        relay_request
            .headers_mut()
            .insert("X-Robot-API-Key", header_value);

        let relay_ws = match tokio_tungstenite::connect_async(relay_request).await {
            Ok((stream, _)) => {
                tracing::info!(
                    "[relay] connected: robot={} session_id={}",
                    robot_id,
                    relay_session_id
                );
                stream
            }
            Err(err) => {
                tracing::warn!(
                    "[relay] connect failed: robot={} session_id={} err={:#}",
                    robot_id,
                    relay_session_id,
                    err
                );
                tokio::select! {
                    _ = sleep(Duration::from_secs(2)) => {}
                    _ = shutdown_rx.changed() => {}
                }
                continue;
            }
        };

        let (mut relay_write, mut relay_read) = relay_ws.split();
        let (relay_closed_tx, mut relay_closed_rx) = mpsc::channel::<String>(128);
        let mut relays: HashMap<String, RelaySession> = HashMap::new();
        let closed_relays = closed_relays.clone();
        let mut relay_ping_tick = tokio::time::interval(Duration::from_secs(30));
        let mut relay_watchdog_tick = tokio::time::interval(Duration::from_secs(10));
        let mut last_relay_inbound = Instant::now();
        let relay_inbound_timeout = Duration::from_secs(75);
        let mut shutdown_now = false;
        let mut scheduler = RelayOutboundScheduler::new(RELAY_DATA_QUEUE_LIMIT);
        let mut reconnect_relay = false;

        loop {
            while let Ok(outbound) = relay_outbound_rx.try_recv() {
                if should_drop_closed_relay_outbound(&outbound, &closed_relays).await {
                    continue;
                }
                enqueue_relay_outbound(&mut scheduler, outbound, &relay_stats).await;
            }
            if let Some(outbound) = scheduler.pop_next() {
                record_relay_scheduler_lag(
                    &relay_stats,
                    &outbound.relay_id,
                    outbound.enqueued_at.elapsed(),
                )
                .await;
                match timeout(
                    relay_write_timeout,
                    relay_write.send(Message::Text(outbound.payload.clone().into())),
                )
                .await
                {
                    Ok(Ok(())) => {
                        record_relay_sent(&relay_stats, &outbound.relay_id).await;
                        continue;
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(
                            "[relay] outbound write failed: robot={} session_id={} relay_id={} type={} err={}",
                            robot_id,
                            relay_session_id,
                            outbound.relay_id,
                            outbound.msg_type,
                            err
                        );
                        reconnect_relay = true;
                        break;
                    }
                    Err(_) => {
                        tracing::warn!(
                            "[relay] outbound write timeout: robot={} session_id={} relay_id={} type={} timeout={}s",
                            robot_id,
                            relay_session_id,
                            outbound.relay_id,
                            outbound.msg_type,
                            relay_write_timeout.as_secs()
                        );
                        reconnect_relay = true;
                        break;
                    }
                }
            }
            tokio::select! {
                _ = relay_ping_tick.tick() => {
                    match timeout(relay_write_timeout, relay_write.send(Message::Ping(Vec::new().into()))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            tracing::warn!(
                                "[relay] ping failed: robot={} session_id={} err={}",
                                robot_id,
                                relay_session_id,
                                err
                            );
                            reconnect_relay = true;
                            break;
                        }
                        Err(_) => {
                            tracing::warn!(
                                "[relay] ping write timeout: robot={} session_id={} timeout={}s",
                                robot_id,
                                relay_session_id,
                                relay_write_timeout.as_secs()
                            );
                            reconnect_relay = true;
                            break;
                        }
                    }
                }
                _ = relay_watchdog_tick.tick() => {
                    if last_relay_inbound.elapsed() > relay_inbound_timeout {
                        tracing::warn!(
                            "[relay] watchdog timeout: robot={} session_id={} no inbound message for {}s, reconnecting",
                            robot_id,
                            relay_session_id,
                            relay_inbound_timeout.as_secs()
                        );
                        reconnect_relay = true;
                        break;
                    }
                }
                maybe_outbound = relay_outbound_rx.recv() => {
                    let outbound = match maybe_outbound {
                        Some(v) => v,
                        None => {
                            shutdown_now = true;
                            break;
                        }
                    };
                    if should_drop_closed_relay_outbound(&outbound, &closed_relays).await {
                        continue;
                    }
                    enqueue_relay_outbound(&mut scheduler, outbound, &relay_stats).await;
                }
                Some(relay_id) = relay_closed_rx.recv() => {
                    mark_relay_closed(&closed_relays, &relay_id).await;
                    let purged = scheduler.purge_relay(&relay_id);
                    if purged.dropped_control > 0 || purged.dropped_data > 0 {
                        tracing::info!(
                            "[relay] purged queued outbound after local close relay={} control={} data={}",
                            relay_id,
                            purged.dropped_control,
                            purged.dropped_data
                        );
                    }
                    if let Some(relay) = relays.remove(&relay_id) {
                        stop_relay_session(&relay_id, relay, "relay_local_close_signal").await;
                    }
                }
                maybe_msg = relay_read.next() => {
                    match maybe_msg {
                        Some(Ok(Message::Text(text))) => {
                            last_relay_inbound = Instant::now();
                            match serde_json::from_str::<AgentControlInbound>(&text) {
                                Ok(msg) => {
                                    match msg.msg_type.as_str() {
                                        "relay_open" | "relay_data" | "relay_close" => {
                                            handle_relay_inbound_message(
                                                msg,
                                                &mut relays,
                                                &mut scheduler,
                                                &relay_outbound_tx,
                                                &relay_closed_tx,
                                                &closed_relays,
                                                &relay_stats,
                                                "relay",
                                            ).await;
                                        }
                                        other => {
                                            tracing::debug!("[relay] ignoring message type={}", other);
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!("[relay] invalid relay message: {}", err);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            last_relay_inbound = Instant::now();
                            match timeout(relay_write_timeout, relay_write.send(Message::Pong(payload))).await {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => {
                                    tracing::warn!(
                                        "[relay] pong write failed: robot={} session_id={} err={}",
                                        robot_id,
                                        relay_session_id,
                                        err
                                    );
                                    break;
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        "[relay] pong write timeout: robot={} session_id={} timeout={}s",
                                        robot_id,
                                        relay_session_id,
                                        relay_write_timeout.as_secs()
                                    );
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            last_relay_inbound = Instant::now();
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::info!(
                                "[relay] platform closed relay channel: robot={} session_id={}",
                                robot_id,
                                relay_session_id
                            );
                            reconnect_relay = true;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            tracing::warn!(
                                "[relay] read failed: robot={} session_id={} err={}",
                                robot_id,
                                relay_session_id,
                                err
                            );
                            reconnect_relay = true;
                            break;
                        }
                        None => {
                            tracing::info!(
                                "[relay] channel ended: robot={} session_id={}",
                                robot_id,
                                relay_session_id
                            );
                            reconnect_relay = true;
                            break;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    shutdown_now = true;
                    break;
                }
            }
        }

        for (relay_id, relay) in relays.drain() {
            let purged = scheduler.purge_relay(&relay_id);
            if purged.dropped_control > 0 || purged.dropped_data > 0 {
                tracing::info!(
                    "[relay] purged queued outbound during relay teardown relay={} control={} data={}",
                    relay_id,
                    purged.dropped_control,
                    purged.dropped_data
                );
            }
            stop_relay_session(&relay_id, relay, "relay_plane_teardown").await;
        }
        let _ = relay_write.send(Message::Close(None)).await;
        log_relay_queue_stats(&relay_stats, &robot_id, &relay_session_id).await;

        if shutdown_now || *shutdown_rx.borrow() {
            return;
        }
        if !reconnect_relay {
            return;
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(1)) => {}
            _ = shutdown_rx.changed() => {}
        }
    }
}

async fn handle_relay_inbound_message(
    msg: AgentControlInbound,
    relays: &mut HashMap<String, RelaySession>,
    scheduler: &mut RelayOutboundScheduler,
    relay_outbound_tx: &mpsc::Sender<RelayOutboundEnvelope>,
    relay_closed_tx: &mpsc::Sender<String>,
    closed_relays: &ClosedRelaySet,
    relay_stats: &RelayQueueStatsMap,
    source: &str,
) {
    match msg.msg_type.as_str() {
        "relay_open" => {
            let relay_id = normalize_optional(msg.relay_id.clone());
            let port = msg.port.unwrap_or_default();
            let open_started = Instant::now();
            if relay_id.is_none() || port == 0 {
                let outbound = relay_control_envelope(
                    "relay_open_ack",
                    relay_id.as_deref().unwrap_or(""),
                    None,
                    Some("error"),
                    Some("invalid relay_open payload"),
                );
                let _ = send_relay_outbound(relay_outbound_tx, relay_stats, outbound).await;
                return;
            }
            let relay_id = relay_id.unwrap_or_default();
            clear_relay_closed(closed_relays, &relay_id).await;
            if let Some(prev) = relays.remove(&relay_id) {
                let purged = scheduler.purge_relay(&relay_id);
                if purged.dropped_control > 0 || purged.dropped_data > 0 {
                    tracing::info!(
                        "[relay] purged queued outbound before relay reopen relay={} control={} data={}",
                        relay_id,
                        purged.dropped_control,
                        purged.dropped_data
                    );
                }
                stop_relay_session(&relay_id, prev, "relay_reopen_replace").await;
            }
            match open_local_relay(
                relay_id.clone(),
                port,
                relay_outbound_tx.clone(),
                relay_closed_tx.clone(),
                closed_relays.clone(),
                relay_stats.clone(),
            )
            .await
            {
                Ok(relay) => {
                    relays.insert(relay_id.clone(), relay);
                    tracing::info!(
                        "[relay] relay_open ok source={} relay={} port={} elapsed_ms={}",
                        source,
                        relay_id,
                        port,
                        open_started.elapsed().as_millis()
                    );
                    let outbound =
                        relay_control_envelope("relay_open_ack", &relay_id, None, Some("ok"), None);
                    let _ = send_relay_outbound(relay_outbound_tx, relay_stats, outbound).await;
                }
                Err(err) => {
                    tracing::warn!(
                        "[relay] open local relay failed source={} relay={} port={} elapsed_ms={} err={}",
                        source,
                        relay_id,
                        port,
                        open_started.elapsed().as_millis(),
                        err
                    );
                    let outbound = relay_control_envelope(
                        "relay_open_ack",
                        &relay_id,
                        None,
                        Some("error"),
                        Some(&err),
                    );
                    let _ = send_relay_outbound(relay_outbound_tx, relay_stats, outbound).await;
                }
            }
        }
        "relay_data" => {
            let relay_id = normalize_optional(msg.relay_id.clone()).unwrap_or_default();
            let raw = normalize_optional(msg.data.clone()).unwrap_or_default();
            if relay_id.is_empty() || raw.is_empty() {
                return;
            }
            let decoded = match base64::engine::general_purpose::STANDARD.decode(raw.as_bytes()) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        "[relay] relay_data decode failed source={} relay={} err={}",
                        source,
                        relay_id,
                        err
                    );
                    return;
                }
            };
            let relay_write_tx = relays.get(&relay_id).map(|relay| relay.write_tx.clone());
            if let Some(write_tx) = relay_write_tx {
                if write_tx.send(decoded).await.is_err() {
                    if let Some(stale) = relays.remove(&relay_id) {
                        let purged = scheduler.purge_relay(&relay_id);
                        if purged.dropped_control > 0 || purged.dropped_data > 0 {
                            tracing::info!(
                                "[relay] purged queued outbound after write channel close relay={} control={} data={}",
                                relay_id,
                                purged.dropped_control,
                                purged.dropped_data
                            );
                        }
                        stop_relay_session(&relay_id, stale, "relay_write_channel_closed").await;
                    }
                    let outbound = relay_control_envelope(
                        "relay_close",
                        &relay_id,
                        None,
                        None,
                        Some("relay write channel closed"),
                    );
                    let _ = send_relay_outbound(relay_outbound_tx, relay_stats, outbound).await;
                }
            } else {
                let outbound = relay_control_envelope(
                    "relay_close",
                    &relay_id,
                    None,
                    None,
                    Some("relay not found"),
                );
                let _ = send_relay_outbound(relay_outbound_tx, relay_stats, outbound).await;
            }
        }
        "relay_close" => {
            let relay_id = normalize_optional(msg.relay_id.clone()).unwrap_or_default();
            if relay_id.is_empty() {
                return;
            }
            let close_reason = normalize_optional(msg.error.clone())
                .unwrap_or_else(|| "upstream relay_close".to_string());
            tracing::info!(
                "[relay] relay_close received source={} relay={} reason={}",
                source,
                relay_id,
                close_reason
            );
            mark_relay_closed(closed_relays, &relay_id).await;
            let purged = scheduler.purge_relay(&relay_id);
            if purged.dropped_control > 0 || purged.dropped_data > 0 {
                tracing::info!(
                    "[relay] purged queued outbound on relay_close relay={} control={} data={}",
                    relay_id,
                    purged.dropped_control,
                    purged.dropped_data
                );
            }
            if let Some(relay) = relays.remove(&relay_id) {
                stop_relay_session(&relay_id, relay, &close_reason).await;
            }
        }
        _ => {}
    }
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

type ClosedRelaySet = Arc<Mutex<HashSet<String>>>;

async fn mark_relay_closed(closed_relays: &ClosedRelaySet, relay_id: &str) {
    let relay_id = relay_id.trim();
    if relay_id.is_empty() {
        return;
    }
    closed_relays.lock().await.insert(relay_id.to_string());
}

async fn clear_relay_closed(closed_relays: &ClosedRelaySet, relay_id: &str) {
    let relay_id = relay_id.trim();
    if relay_id.is_empty() {
        return;
    }
    closed_relays.lock().await.remove(relay_id);
}

async fn await_relay_task_with_abort(
    mut task: JoinHandle<()>,
    relay_id: &str,
    task_name: &str,
) {
    if timeout(RELAY_CLOSE_GRACE, &mut task).await.is_ok() {
        return;
    }
    tracing::debug!(
        "[relay] graceful close timed out relay={} task={} grace_ms={}, aborting",
        relay_id,
        task_name,
        RELAY_CLOSE_GRACE.as_millis()
    );
    task.abort();
    let _ = task.await;
}

async fn stop_relay_session(relay_id: &str, relay: RelaySession, reason: &str) {
    let relay_id = relay_id.trim();
    let reason = reason.trim();
    let RelaySession {
        write_tx,
        close_tx,
        reader_task,
        writer_task,
    } = relay;
    let _ = close_tx.send(true);
    drop(write_tx);
    await_relay_task_with_abort(writer_task, relay_id, "writer").await;
    await_relay_task_with_abort(reader_task, relay_id, "reader").await;
    tracing::debug!(
        "[relay] relay session stopped relay={} reason={}",
        relay_id,
        if reason.is_empty() { "unspecified" } else { reason }
    );
}

async fn should_drop_closed_relay_outbound(
    outbound: &RelayOutboundEnvelope,
    closed_relays: &ClosedRelaySet,
) -> bool {
    if outbound.msg_type.trim() != "relay_data" {
        return false;
    }
    let relay_id = outbound.relay_id.trim();
    if relay_id.is_empty() {
        return false;
    }
    closed_relays.lock().await.contains(relay_id)
}

async fn enqueue_relay_outbound(
    scheduler: &mut RelayOutboundScheduler,
    outbound: RelayOutboundEnvelope,
    relay_stats: &RelayQueueStatsMap,
) {
    let relay_id = outbound.relay_id.clone();
    let outcome = scheduler.enqueue(outbound);
    let mut guard = relay_stats.lock().await;
    let entry = guard.entry(relay_id).or_default();
    entry.max_queue_depth = entry.max_queue_depth.max(outcome.queue_depth);
    if outcome.dropped_data {
        entry.dropped_data = entry.dropped_data.saturating_add(1);
    }
}

async fn record_relay_enqueue_wait(
    relay_stats: &RelayQueueStatsMap,
    relay_id: &str,
    enqueue_wait: Duration,
) {
    let relay_id = relay_id.trim();
    if relay_id.is_empty() || enqueue_wait.is_zero() {
        return;
    }
    let wait_ms = duration_to_millis_u64(enqueue_wait);
    let mut guard = relay_stats.lock().await;
    let entry = guard.entry(relay_id.to_string()).or_default();
    entry.enqueue_wait_samples += 1;
    entry.enqueue_wait_total_ms = entry.enqueue_wait_total_ms.saturating_add(wait_ms);
    entry.enqueue_wait_max_ms = entry.enqueue_wait_max_ms.max(wait_ms);
}

async fn record_relay_scheduler_lag(
    relay_stats: &RelayQueueStatsMap,
    relay_id: &str,
    scheduler_lag: Duration,
) {
    let relay_id = relay_id.trim();
    if relay_id.is_empty() || scheduler_lag.is_zero() {
        return;
    }
    let lag_ms = duration_to_millis_u64(scheduler_lag);
    let mut guard = relay_stats.lock().await;
    let entry = guard.entry(relay_id.to_string()).or_default();
    entry.scheduler_lag_samples += 1;
    entry.scheduler_lag_total_ms = entry.scheduler_lag_total_ms.saturating_add(lag_ms);
    entry.scheduler_lag_max_ms = entry.scheduler_lag_max_ms.max(lag_ms);
}

async fn record_relay_sent(relay_stats: &RelayQueueStatsMap, relay_id: &str) {
    let relay_id = relay_id.trim();
    if relay_id.is_empty() {
        return;
    }
    let mut guard = relay_stats.lock().await;
    let entry = guard.entry(relay_id.to_string()).or_default();
    entry.sent += 1;
}

async fn send_relay_outbound(
    relay_outbound_tx: &mpsc::Sender<RelayOutboundEnvelope>,
    relay_stats: &RelayQueueStatsMap,
    outbound: RelayOutboundEnvelope,
) -> Result<Duration, mpsc::error::SendError<RelayOutboundEnvelope>> {
    let relay_id = outbound.relay_id.clone();
    let msg_type = outbound.msg_type.trim().to_string();
    let send_started = Instant::now();
    if msg_type == "relay_data" {
        match relay_outbound_tx.try_send(outbound) {
            Ok(()) => {
                let enqueue_wait = send_started.elapsed();
                record_relay_enqueue_wait(relay_stats, &relay_id, enqueue_wait).await;
                return Ok(enqueue_wait);
            }
            Err(TrySendError::Full(_)) => {
                // Drop newest relay_data when the shared outbound channel is full
                // so control traffic can still make progress.
                let enqueue_wait = send_started.elapsed();
                record_relay_enqueue_wait(relay_stats, &relay_id, enqueue_wait).await;
                let mut guard = relay_stats.lock().await;
                let entry = guard.entry(relay_id).or_default();
                entry.dropped_data = entry.dropped_data.saturating_add(1);
                return Ok(enqueue_wait);
            }
            Err(TrySendError::Closed(msg)) => {
                return Err(mpsc::error::SendError(msg));
            }
        }
    }

    let result = relay_outbound_tx.send(outbound).await;
    let enqueue_wait = send_started.elapsed();
    record_relay_enqueue_wait(relay_stats, &relay_id, enqueue_wait).await;
    result.map(|_| enqueue_wait)
}

async fn log_relay_queue_stats(relay_stats: &RelayQueueStatsMap, robot_id: &str, session_id: &str) {
    let snapshot = relay_stats.lock().await.clone();
    let mut relay_ids: Vec<String> = snapshot.keys().cloned().collect();
    relay_ids.sort();
    for relay_id in relay_ids {
        let Some(stat) = snapshot.get(&relay_id) else {
            continue;
        };
        let enqueue_wait_avg = if stat.enqueue_wait_samples > 0 {
            stat.enqueue_wait_total_ms / stat.enqueue_wait_samples
        } else {
            0
        };
        let scheduler_lag_avg = if stat.scheduler_lag_samples > 0 {
            stat.scheduler_lag_total_ms / stat.scheduler_lag_samples
        } else {
            0
        };
        tracing::info!(
            "[relay] queue stats robot={} session_id={} relay={} sent={} dropped_data={} queue_depth_max={} enqueue_wait_avg_ms={} enqueue_wait_max_ms={} scheduler_lag_avg_ms={} scheduler_lag_max_ms={}",
            robot_id,
            session_id,
            relay_id,
            stat.sent,
            stat.dropped_data,
            stat.max_queue_depth,
            enqueue_wait_avg,
            stat.enqueue_wait_max_ms,
            scheduler_lag_avg,
            stat.scheduler_lag_max_ms
        );
    }
}

async fn open_local_relay(
    relay_id: String,
    port: u16,
    outbound_tx: mpsc::Sender<RelayOutboundEnvelope>,
    relay_closed_tx: mpsc::Sender<String>,
    closed_relays: ClosedRelaySet,
    relay_stats: RelayQueueStatsMap,
) -> Result<RelaySession, String> {
    let target = format!("127.0.0.1:{}", port);
    let stream = TcpStream::connect(&target)
        .await
        .map_err(|err| format!("connect {} failed: {}", target, err))?;
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(256);
    let (close_tx, close_rx) = watch::channel(false);

    let relay_id_writer = relay_id.clone();
    let relay_closed_writer = relay_closed_tx.clone();
    let closed_relays_writer = closed_relays.clone();
    let mut close_rx_writer = close_rx.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_chunk = write_rx.recv() => {
                    let Some(chunk) = maybe_chunk else {
                        break;
                    };
                    if let Err(err) = write_half.write_all(&chunk).await {
                        tracing::warn!(
                            "[relay] relay writer failed relay={} err={}",
                            relay_id_writer,
                            err
                        );
                        break;
                    }
                }
                _ = close_rx_writer.changed() => {
                    if *close_rx_writer.borrow() {
                        break;
                    }
                }
            }
        }
        let _ = write_half.shutdown().await;
        mark_relay_closed(&closed_relays_writer, &relay_id_writer).await;
        let _ = relay_closed_writer.send(relay_id_writer).await;
    });

    let relay_id_reader = relay_id.clone();
    let relay_closed_reader = relay_closed_tx.clone();
    let outbound_reader = outbound_tx.clone();
    let closed_relays_reader = closed_relays.clone();
    let relay_stats_reader = relay_stats.clone();
    let mut close_rx_reader = close_rx;
    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        let mut backpressure_active = false;
        let mut last_backpressure_log_at: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = close_rx_reader.changed() => {
                    if *close_rx_reader.borrow() {
                        break;
                    }
                }
                read_result = read_half.read(&mut buf) => match read_result {
                Ok(0) => {
                    mark_relay_closed(&closed_relays_reader, &relay_id_reader).await;
                    let outbound = relay_control_envelope(
                        "relay_close",
                        &relay_id_reader,
                        None,
                        None,
                        Some("relay target closed"),
                    );
                    let _ =
                        send_relay_outbound(&outbound_reader, &relay_stats_reader, outbound).await;
                    break;
                }
                Ok(n) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                    let outbound = relay_control_envelope(
                        "relay_data",
                        &relay_id_reader,
                        Some(encoded),
                        None,
                        None,
                    );
                    let enqueue_wait =
                        match send_relay_outbound(&outbound_reader, &relay_stats_reader, outbound)
                            .await
                        {
                            Ok(wait) => wait,
                            Err(_) => {
                                break;
                            }
                        };
                    if enqueue_wait >= Duration::from_millis(50) {
                        let now = Instant::now();
                        let should_log = match last_backpressure_log_at {
                            Some(last) => now.duration_since(last) >= Duration::from_secs(2),
                            None => true,
                        };
                        if should_log {
                            tracing::warn!(
                                "[relay] relay outbound backpressure relay={} enqueue_wait_ms={}",
                                relay_id_reader,
                                enqueue_wait.as_millis()
                            );
                            last_backpressure_log_at = Some(now);
                        }
                        backpressure_active = true;
                    } else if backpressure_active {
                        tracing::info!(
                            "[relay] relay outbound recovered relay={} enqueue_wait_ms={}",
                            relay_id_reader,
                            enqueue_wait.as_millis()
                        );
                        backpressure_active = false;
                        last_backpressure_log_at = None;
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "[relay] relay reader failed relay={} err={}",
                        relay_id_reader,
                        err
                    );
                    mark_relay_closed(&closed_relays_reader, &relay_id_reader).await;
                    let outbound = relay_control_envelope(
                        "relay_close",
                        &relay_id_reader,
                        None,
                        None,
                        Some(&format!("relay read error: {}", err)),
                    );
                    let _ =
                        send_relay_outbound(&outbound_reader, &relay_stats_reader, outbound).await;
                    break;
                }
                }
            }
        }
        mark_relay_closed(&closed_relays_reader, &relay_id_reader).await;
        let _ = relay_closed_reader.send(relay_id_reader).await;
    });

    Ok(RelaySession {
        write_tx,
        close_tx,
        reader_task,
        writer_task,
    })
}

async fn open_local_webrtc_relay(
    relay_id: String,
    port: u16,
    outbound_tx: mpsc::Sender<Vec<u8>>,
    relay_closed_tx: mpsc::Sender<String>,
) -> Result<RelaySession, String> {
    let target = format!("127.0.0.1:{}", port);
    let stream = TcpStream::connect(&target)
        .await
        .map_err(|err| format!("connect {} failed: {}", target, err))?;
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(256);
    let (close_tx, close_rx) = watch::channel(false);

    let relay_id_writer = relay_id.clone();
    let relay_closed_writer = relay_closed_tx.clone();
    let mut close_rx_writer = close_rx.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_chunk = write_rx.recv() => {
                    let Some(chunk) = maybe_chunk else {
                        break;
                    };
                    if let Err(err) = write_half.write_all(&chunk).await {
                        tracing::warn!(
                            "[webrtc] relay writer failed relay={} err={}",
                            relay_id_writer,
                            err
                        );
                        break;
                    }
                }
                _ = close_rx_writer.changed() => {
                    if *close_rx_writer.borrow() {
                        break;
                    }
                }
            }
        }
        let _ = write_half.shutdown().await;
        let _ = relay_closed_writer.send(relay_id_writer).await;
    });

    let relay_id_reader = relay_id.clone();
    let relay_closed_reader = relay_closed_tx.clone();
    let outbound_reader = outbound_tx.clone();
    let mut close_rx_reader = close_rx;
    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            tokio::select! {
                _ = close_rx_reader.changed() => {
                    if *close_rx_reader.borrow() {
                        break;
                    }
                }
                read_result = read_half.read(&mut buf) => match read_result {
                Ok(0) => {
                    let _ = send_webrtc_relay_event(
                        &outbound_reader,
                        FrameType::RelayClose,
                        &WebRtcRelayFrame {
                            relay_id: relay_id_reader.clone(),
                            error: Some("relay target closed".to_string()),
                            ..Default::default()
                        },
                    )
                    .await;
                    break;
                }
                Ok(n) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                    let _ = send_webrtc_relay_event(
                        &outbound_reader,
                        FrameType::RelayData,
                        &WebRtcRelayFrame {
                            relay_id: relay_id_reader.clone(),
                            data: Some(encoded),
                            ..Default::default()
                        },
                    )
                    .await;
                }
                Err(err) => {
                    tracing::warn!(
                        "[webrtc] relay reader failed relay={} err={}",
                        relay_id_reader,
                        err
                    );
                    let _ = send_webrtc_relay_event(
                        &outbound_reader,
                        FrameType::RelayClose,
                        &WebRtcRelayFrame {
                            relay_id: relay_id_reader.clone(),
                            error: Some(format!("relay read error: {}", err)),
                            ..Default::default()
                        },
                    )
                    .await;
                    break;
                }
                }
            }
        }
        let _ = relay_closed_reader.send(relay_id_reader).await;
    });

    Ok(RelaySession {
        write_tx,
        close_tx,
        reader_task,
        writer_task,
    })
}

async fn send_webrtc_relay_event(
    outbound_tx: &mpsc::Sender<Vec<u8>>,
    frame_type: FrameType,
    frame: &WebRtcRelayFrame,
) -> Result<(), String> {
    let data = serde_json::to_vec(frame)
        .map_err(|err| format!("serialize webrtc relay payload failed: {}", err))?;
    let encoded = encode_v2_frame(frame_type, &data);
    outbound_tx
        .send(encoded)
        .await
        .map_err(|_| "webrtc relay outbound channel closed".to_string())
}

fn relay_control_envelope(
    msg_type: &str,
    relay_id: &str,
    data: Option<String>,
    status: Option<&str>,
    error: Option<&str>,
) -> RelayOutboundEnvelope {
    let relay_id = relay_id.trim().to_string();
    RelayOutboundEnvelope {
        msg_type: msg_type.trim().to_string(),
        relay_id: relay_id.clone(),
        payload: relay_control_payload(msg_type, &relay_id, data, status, error),
        enqueued_at: Instant::now(),
    }
}

fn relay_control_payload(
    msg_type: &str,
    relay_id: &str,
    data: Option<String>,
    status: Option<&str>,
    error: Option<&str>,
) -> String {
    let mut outbound = serde_json::Map::new();
    outbound.insert(
        "type".to_string(),
        serde_json::Value::String(msg_type.to_string()),
    );
    outbound.insert(
        "relay_id".to_string(),
        serde_json::Value::String(relay_id.to_string()),
    );
    if let Some(v) = data {
        outbound.insert("data".to_string(), serde_json::Value::String(v));
    }
    if let Some(v) = status {
        outbound.insert(
            "status".to_string(),
            serde_json::Value::String(v.to_string()),
        );
    }
    if let Some(v) = error {
        outbound.insert(
            "error".to_string(),
            serde_json::Value::String(v.to_string()),
        );
    }
    serde_json::Value::Object(outbound).to_string()
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
    relay_ctx: Arc<WebRtcRelayContext>,
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
            FrameType::RelayOpen => {
                let req = match serde_json::from_slice::<WebRtcRelayFrame>(frame_data) {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!("webrtc: invalid relay_open payload: {}", err);
                        return;
                    }
                };
                let relay_id = req.relay_id.trim().to_string();
                let port = req.port.unwrap_or_default();
                let result = relay_ctx.open_relay(relay_id.clone(), port).await;
                let (status, error) = match result {
                    Ok(()) => ("ok".to_string(), None),
                    Err(err) => ("error".to_string(), Some(err)),
                };
                let _ = send_webrtc_relay_event(
                    &relay_ctx.outbound_tx,
                    FrameType::RelayOpenAck,
                    &WebRtcRelayFrame {
                        relay_id,
                        status: Some(status),
                        error,
                        ..Default::default()
                    },
                )
                .await;
            }
            FrameType::RelayData => {
                let req = match serde_json::from_slice::<WebRtcRelayFrame>(frame_data) {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!("webrtc: invalid relay_data payload: {}", err);
                        return;
                    }
                };
                let relay_id = req.relay_id.trim().to_string();
                let raw = req.data.unwrap_or_default();
                if relay_id.is_empty() || raw.trim().is_empty() {
                    return;
                }
                let decoded = match base64::engine::general_purpose::STANDARD.decode(raw.as_bytes())
                {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::warn!(
                            "webrtc: relay_data decode failed relay={} err={}",
                            relay_id,
                            err
                        );
                        return;
                    }
                };
                if let Err(err) = relay_ctx.write_relay(&relay_id, decoded).await {
                    let _ = send_webrtc_relay_event(
                        &relay_ctx.outbound_tx,
                        FrameType::RelayClose,
                        &WebRtcRelayFrame {
                            relay_id,
                            error: Some(err),
                            ..Default::default()
                        },
                    )
                    .await;
                }
            }
            FrameType::RelayClose => {
                let req = match serde_json::from_slice::<WebRtcRelayFrame>(frame_data) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                relay_ctx.close_relay(&req.relay_id).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drops_stale_relay_data_after_close() {
        let closed_relays: ClosedRelaySet = Arc::new(Mutex::new(HashSet::new()));
        mark_relay_closed(&closed_relays, "relay-1").await;

        let outbound = relay_control_envelope(
            "relay_data",
            "relay-1",
            Some("Zm9v".to_string()),
            None,
            None,
        );

        assert!(should_drop_closed_relay_outbound(&outbound, &closed_relays).await);
    }

    #[tokio::test]
    async fn keeps_non_data_or_other_relay_messages() {
        let closed_relays: ClosedRelaySet = Arc::new(Mutex::new(HashSet::new()));
        mark_relay_closed(&closed_relays, "relay-1").await;

        let close_msg = relay_control_envelope(
            "relay_close",
            "relay-1",
            None,
            None,
            Some("relay target closed"),
        );
        let other_data = relay_control_envelope(
            "relay_data",
            "relay-2",
            Some("YmFy".to_string()),
            None,
            None,
        );

        assert!(!should_drop_closed_relay_outbound(&close_msg, &closed_relays).await);
        assert!(!should_drop_closed_relay_outbound(&other_data, &closed_relays).await);
    }

    #[tokio::test]
    async fn relay_inbound_open_data_close_emits_expected_outbound() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(b"foxglove").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<RelayOutboundEnvelope>(16);
        let (relay_closed_tx, _relay_closed_rx) = mpsc::channel::<String>(8);
        let closed_relays: ClosedRelaySet = Arc::new(Mutex::new(HashSet::new()));
        let relay_stats: RelayQueueStatsMap = Arc::new(Mutex::new(HashMap::new()));
        let mut relays: HashMap<String, RelaySession> = HashMap::new();
        let mut scheduler = RelayOutboundScheduler::new(RELAY_DATA_QUEUE_LIMIT);

        handle_relay_inbound_message(
            AgentControlInbound {
                msg_type: "relay_open".to_string(),
                bootstrap_id: None,
                session_key: None,
                cli_public_ip: None,
                cli_lan_cidr: None,
                route_type: None,
                request_id: None,
                command: None,
                status_query: None,
                relay_id: Some("relay-control-1".to_string()),
                port: Some(port),
                data: None,
                error: None,
            },
            &mut relays,
            &mut scheduler,
            &outbound_tx,
            &relay_closed_tx,
            &closed_relays,
            &relay_stats,
            "relay",
        )
        .await;

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut saw_open_ack_ok = false;
        let mut saw_data = false;
        while Instant::now() < deadline && !(saw_open_ack_ok && saw_data) {
            let maybe_msg =
                tokio::time::timeout(Duration::from_millis(200), outbound_rx.recv()).await;
            let Some(msg) = maybe_msg.ok().flatten() else {
                continue;
            };
            if msg.msg_type == "relay_open_ack" {
                let payload: serde_json::Value = serde_json::from_str(&msg.payload).unwrap();
                if payload.get("status").and_then(serde_json::Value::as_str) == Some("ok") {
                    saw_open_ack_ok = true;
                }
            }
            if msg.msg_type == "relay_data" && msg.relay_id == "relay-control-1" {
                saw_data = true;
            }
        }
        assert!(saw_open_ack_ok, "expected relay_open_ack status=ok");
        assert!(saw_data, "expected at least one relay_data frame");

        handle_relay_inbound_message(
            AgentControlInbound {
                msg_type: "relay_close".to_string(),
                bootstrap_id: None,
                session_key: None,
                cli_public_ip: None,
                cli_lan_cidr: None,
                route_type: None,
                request_id: None,
                command: None,
                status_query: None,
                relay_id: Some("relay-control-1".to_string()),
                port: None,
                data: None,
                error: None,
            },
            &mut relays,
            &mut scheduler,
            &outbound_tx,
            &relay_closed_tx,
            &closed_relays,
            &relay_stats,
            "relay",
        )
        .await;

        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn relay_close_purges_queued_data_for_same_relay() {
        let (outbound_tx, _outbound_rx) = mpsc::channel::<RelayOutboundEnvelope>(16);
        let (relay_closed_tx, _relay_closed_rx) = mpsc::channel::<String>(8);
        let closed_relays: ClosedRelaySet = Arc::new(Mutex::new(HashSet::new()));
        let relay_stats: RelayQueueStatsMap = Arc::new(Mutex::new(HashMap::new()));
        let mut relays: HashMap<String, RelaySession> = HashMap::new();
        let mut scheduler = RelayOutboundScheduler::new(8);

        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("YQ==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-b",
            Some("Yg==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("Yw==".to_string()),
            None,
            None,
        ));

        handle_relay_inbound_message(
            AgentControlInbound {
                msg_type: "relay_close".to_string(),
                bootstrap_id: None,
                session_key: None,
                cli_public_ip: None,
                cli_lan_cidr: None,
                route_type: None,
                request_id: None,
                command: None,
                status_query: None,
                relay_id: Some("relay-a".to_string()),
                port: None,
                data: None,
                error: Some("client_disconnected".to_string()),
            },
            &mut relays,
            &mut scheduler,
            &outbound_tx,
            &relay_closed_tx,
            &closed_relays,
            &relay_stats,
            "relay",
        )
        .await;

        let mut seen = Vec::new();
        while let Some(msg) = scheduler.pop_next() {
            seen.push(msg.relay_id);
        }
        assert_eq!(seen, vec!["relay-b".to_string()]);
        assert!(closed_relays.lock().await.contains("relay-a"));
    }

    #[test]
    fn relay_scheduler_round_robin_across_relays() {
        let mut scheduler = RelayOutboundScheduler::new(8);
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("YQ==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("Yg==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-b",
            Some("Yw==".to_string()),
            None,
            None,
        ));

        let first = scheduler.pop_next().unwrap().relay_id;
        let second = scheduler.pop_next().unwrap().relay_id;
        let third = scheduler.pop_next().unwrap().relay_id;
        assert_eq!(first, "relay-a");
        assert_eq!(second, "relay-b");
        assert_eq!(third, "relay-a");
    }

    #[test]
    fn relay_scheduler_prioritizes_control_globally() {
        let mut scheduler = RelayOutboundScheduler::new(8);
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("YQ==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-b",
            Some("Yg==".to_string()),
            None,
            None,
        ));
        scheduler.enqueue(relay_control_envelope(
            "relay_open_ack",
            "relay-z",
            None,
            Some("ok"),
            None,
        ));

        let first = scheduler.pop_next().unwrap();
        assert_eq!(first.msg_type, "relay_open_ack");
        assert_eq!(first.relay_id, "relay-z");
    }

    #[test]
    fn relay_scheduler_drops_oldest_data_but_keeps_control_priority() {
        let mut scheduler = RelayOutboundScheduler::new(2);
        let first = scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("MQ==".to_string()),
            None,
            None,
        ));
        assert!(!first.dropped_data);

        let second = scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("Mg==".to_string()),
            None,
            None,
        ));
        assert!(!second.dropped_data);

        let third = scheduler.enqueue(relay_control_envelope(
            "relay_data",
            "relay-a",
            Some("Mw==".to_string()),
            None,
            None,
        ));
        assert!(third.dropped_data);

        scheduler.enqueue(relay_control_envelope(
            "relay_close",
            "relay-a",
            None,
            None,
            Some("done"),
        ));

        let control = scheduler.pop_next().unwrap();
        assert_eq!(control.msg_type, "relay_close");

        let data1 = scheduler.pop_next().unwrap();
        let data2 = scheduler.pop_next().unwrap();
        assert_eq!(data1.msg_type, "relay_data");
        assert_eq!(data2.msg_type, "relay_data");

        let payload1: serde_json::Value = serde_json::from_str(&data1.payload).unwrap();
        let payload2: serde_json::Value = serde_json::from_str(&data2.payload).unwrap();
        assert_eq!(
            payload1.get("data").and_then(serde_json::Value::as_str),
            Some("Mg==")
        );
        assert_eq!(
            payload2.get("data").and_then(serde_json::Value::as_str),
            Some("Mw==")
        );
    }
}
