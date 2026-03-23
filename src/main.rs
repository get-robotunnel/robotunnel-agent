//! RoboTunnel Agent — bootstrap entry point.
//!
//! Layer ownership:
//! - interaction: CLI/web transport ingress
//! - connection: auth, TCP tunnel, WebRTC
//! - platform: platform-facing configuration and APIs
//! - application: robot skills and background services

mod application;
mod interaction;

use application::MonitorSettings;
use clap::{Parser, Subcommand};
use rt_core::authorized_keys::AuthorizedKeysSyncService;
use rt_core::config::AgentConfig;
use rt_core::heartbeat::HeartbeatService;
use rt_core::tunnel::TunnelServer;
use rt_llm::{LlmManager, Provider};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing;
use tracing_subscriber::{fmt, EnvFilter};

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_BUILD_DATE: &str = env!("ROBOTUNNEL_BUILD_DATE");
const APP_GIT_COMMIT: &str = env!("ROBOTUNNEL_GIT_COMMIT");
const APP_LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "\nBuild Date: ",
    env!("ROBOTUNNEL_BUILD_DATE"),
    "\nGit Commit: ",
    env!("ROBOTUNNEL_GIT_COMMIT")
);

#[derive(Parser, Debug)]
#[command(name = "robotunnel-agent")]
#[command(version = APP_VERSION, long_version = APP_LONG_VERSION)]
#[command(about = "RoboTunnel Agent — The Physical World API Layer")]
struct Args {
    /// Path to config file
    #[arg(short, long, global = true)]
    config: Option<String>,

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
    /// Manage local monitor alert settings
    Monitor {
        #[command(subcommand)]
        action: MonitorAction,
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

#[derive(Subcommand, Debug)]
enum MonitorAction {
    /// Persist monitor alert settings on this device
    SetAlert {
        /// Key=value settings such as cpu_threshold=85 notify=discord webhook_url=https://...
        settings: Vec<String>,
    },
    /// Show the current monitor alert settings
    Show,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config_path = resolve_config_path(args.config);

    // Handle key management subcommands (no tunnel needed)
    if let Some(Command::Keys { action }) = args.command {
        return handle_keys(action).await;
    }
    if let Some(Command::Monitor { action }) = args.command {
        return handle_monitor(action).await;
    }

    // --- Agent mode ---

    // Load configuration
    let config = if std::path::Path::new(&config_path).exists() {
        AgentConfig::load_with_env(&config_path)?
    } else {
        eprintln!(
            "[INFO] config file not found at {}, using defaults",
            config_path
        );
        let mut config = AgentConfig::default();
        config.apply_env_overrides();
        config.validate_security()?;
        config
    };

    // Initialize logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));
    fmt().with_env_filter(filter).with_target(false).init();

    tracing::info!(
        "robotunnel-agent v{} starting (build_date={}, git_commit={})",
        APP_VERSION,
        APP_BUILD_DATE,
        APP_GIT_COMMIT
    );

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Command channel: tunnel -> router
    let (cmd_tx, cmd_rx) = mpsc::channel(256);

    if config.server.insecure_allow_any_client {
        tracing::warn!(
            "server.insecure_allow_any_client=true: agent will accept any valid Ed25519 signature"
        );
    } else {
        tracing::info!(
            "server authorized key allowlist enabled ({} key(s))",
            config.server.authorized_keys.len()
        );
    }

    // Build tunnel server
    let mut tunnel_server = TunnelServer::new(
        config.server.listen_port,
        config.server.authorized_keys.clone(),
        cmd_tx.clone(),
    );

    let (webrtc_trigger_tx, webrtc_trigger_rx) = mpsc::channel(256);
    let (webrtc_teardown_tx, webrtc_teardown_rx) = mpsc::channel(256);
    let control_trigger_tx = webrtc_trigger_tx.clone();
    let control_teardown_tx = webrtc_teardown_tx.clone();
    if config.webrtc.enabled {
        tunnel_server.set_webrtc_trigger(webrtc_trigger_tx);
        tunnel_server.set_webrtc_teardown(webrtc_teardown_tx);
    }
    let tunnel_server = Arc::new(tunnel_server);

    let router = application::build_application_router(tunnel_server.broadcast_tx());

    let monitor_handle = application::start_monitor_service_if_enabled(
        shutdown_rx.clone(),
        Some(config.platform.api_url.clone()),
        config.platform.api_key.clone(),
    );

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

    let authorized_keys_handle = if let Some(api_key) = &config.platform.api_key {
        let sync = AuthorizedKeysSyncService::new(
            config.platform.api_url.clone(),
            api_key.clone(),
            config.heartbeat.interval_secs,
            config.server.authorized_keys.clone(),
        );
        let shutdown_rx = shutdown_rx.clone();
        let tunnel_server = tunnel_server.clone();
        Some(tokio::spawn(async move {
            sync.run(tunnel_server, shutdown_rx).await;
        }))
    } else {
        tracing::warn!("no API key configured, authorized key sync disabled");
        None
    };

    let control_plane_handle = interaction::start_control_plane_bridge_if_enabled(
        &config,
        cmd_tx.clone(),
        control_trigger_tx,
        control_teardown_tx,
        shutdown_rx.clone(),
    );

    let webrtc_handle = interaction::start_webrtc_bridge_if_enabled(
        &config,
        cmd_tx.clone(),
        webrtc_trigger_rx,
        webrtc_teardown_rx,
        shutdown_rx.clone(),
    );

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
    if let Some(handle) = authorized_keys_handle {
        handle.abort();
    }
    if let Some(handle) = monitor_handle {
        handle.abort();
    }
    if let Some(handle) = heartbeat_handle {
        handle.abort();
    }
    if let Some(handle) = webrtc_handle {
        handle.abort();
    }
    if let Some(handle) = control_plane_handle {
        handle.abort();
    }
    router_handle.abort();

    tracing::info!("agent shutdown complete");
    Ok(())
}

fn resolve_config_path(cli_path: Option<String>) -> String {
    if let Some(path) = cli_path
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
    {
        return path;
    }

    if let Ok(path) = std::env::var("RT_AGENT_CONFIG") {
        let path = path.trim();
        if !path.is_empty() {
            return path.to_string();
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let user_config = format!("{}/.config/robotunnel/agent.toml", home);
        if std::path::Path::new(&user_config).exists() {
            return user_config;
        }
    }

    "/etc/robotunnel/agent.toml".to_string()
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

async fn handle_monitor(action: MonitorAction) -> anyhow::Result<()> {
    match action {
        MonitorAction::SetAlert { settings } => {
            let mut config = MonitorSettings::load()?;
            let warnings = config.apply_overrides(&settings)?;
            config.save()?;

            println!("✓ Monitor alert settings saved.");
            print_monitor_settings(&config);
            for warning in warnings {
                println!("Warning: {}", warning);
            }
        }
        MonitorAction::Show => {
            let config = MonitorSettings::load()?;
            print_monitor_settings(&config);
        }
    }

    Ok(())
}

fn print_monitor_settings(config: &MonitorSettings) {
    println!("enabled: {}", yes_no(config.enabled));
    println!("sample_interval_secs: {}", config.sample_interval_secs);
    println!(
        "cpu_threshold: {}",
        config
            .cpu_threshold_percent
            .map(|v| format!("{:.1}", v))
            .unwrap_or_else(|| "auto".to_string())
    );
    println!(
        "mem_threshold: {}",
        config
            .mem_threshold_percent
            .map(|v| format!("{:.1}", v))
            .unwrap_or_else(|| "auto".to_string())
    );
    println!("notify: {}", config.notify);
    println!(
        "webhook_url: {}",
        config
            .webhook_url
            .as_deref()
            .map(mask_secret_url)
            .unwrap_or_else(|| "(not set)".to_string())
    );
    println!(
        "provider: {}",
        config.provider.as_deref().unwrap_or("(not set)")
    );
}

fn yes_no(v: bool) -> &'static str {
    if v {
        "yes"
    } else {
        "no"
    }
}

fn mask_secret_url(url: &str) -> String {
    if url.len() <= 16 {
        return "*".repeat(url.len());
    }
    format!("{}...{}", &url[..12], &url[url.len() - 4..])
}
