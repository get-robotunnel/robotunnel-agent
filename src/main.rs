//! RoboTunnel Agent — main entry point.
//!
//! Supports two modes:
//!   1. Agent mode (default): starts the tunnel server, heartbeat, and skill router.
//!   2. Keys mode: manage local encrypted LLM API keys.
//!      robotunnel-agent keys set <provider> <api-key>
//!      robotunnel-agent keys list
//!      robotunnel-agent keys remove <provider>

use clap::{Parser, Subcommand};
use rt_core::config::AgentConfig;
use rt_core::tunnel::TunnelServer;
use rt_core::heartbeat::HeartbeatService;
use rt_runtime::router::Router;
use rt_skill_ros2::Ros2Skill;
use rt_llm::{LlmManager, Provider};
use tokio::sync::{mpsc, watch};
use tracing;
use tracing_subscriber::{EnvFilter, fmt};
use std::sync::Arc;
use rt_runtime::Skill;

#[derive(Parser, Debug)]
#[command(name = "robotunnel-agent")]
#[command(version = "0.3.0")]
#[command(about = "RoboTunnel Agent — The Physical World API Layer")]
struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "/etc/robotunnel/agent.toml", global = true)]
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
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.level));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    tracing::info!("robotunnel-agent v0.3.0 starting");

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Command channel: tunnel -> router
    let (cmd_tx, cmd_rx) = mpsc::channel(256);

    // Build tunnel server
    let tunnel_server = TunnelServer::new(
        config.server.listen_port,
        vec![],
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
    if let Some(handle) = heartbeat_handle { handle.abort(); }
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
            println!("✓ API key set for {} — stored encrypted on this device only.", p.display_name());
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
                println!("\nNote: Keys are encrypted with AES-256-GCM using your machine's hardware ID.");
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
