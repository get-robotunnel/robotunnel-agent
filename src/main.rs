//! RoboTunnel Agent — main entry point.
//!
//! Starts the tunnel server, heartbeat service, skill router,
//! and wires them together for remote robot management.

use clap::Parser;
use rt_core::config::AgentConfig;
use rt_core::tunnel::TunnelServer;
use rt_core::heartbeat::HeartbeatService;
use rt_runtime::router::Router;
use rt_skill_ros2::Ros2Skill;
use tokio::sync::{mpsc, watch};
use tracing;
use tracing_subscriber::{EnvFilter, fmt};
use std::sync::Arc;
use rt_runtime::Skill; // Import Skill trait

#[derive(Parser, Debug)]
#[command(name = "robotunnel-agent")]
#[command(version = "0.2.0")]
#[command(about = "RoboTunnel Agent — Remote robot management with ZeroClaw runtime")]
struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "/etc/robotunnel/agent.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

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

    tracing::info!("robotunnel-agent v0.2.0 starting");

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Command channel: tunnel -> router
    let (cmd_tx, cmd_rx) = mpsc::channel(256);

    // Build tunnel server
    let tunnel_server = TunnelServer::new(
        config.server.listen_port,
        vec![], // Accept any authenticated key for now
        cmd_tx,
    );

    // Build skill router with tunnel's broadcast channel
    let mut router = Router::new(tunnel_server.broadcast_tx());
    
    // 1. Register Debug Skill
    router.register("debug", |req, tx| async move {
        rt_skill_debug::handle(req, tx).await
    });
    tracing::info!("registered skill: debug");

    // 2. Register ROS2 Skill (if rosbridge URL is provided or use default)
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

    // Build heartbeat service (if API key is configured)
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

    // Handle shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("shutdown signal received");
        let _ = shutdown_tx_clone.send(true);
    });

    // Run tunnel server (blocks until shutdown)
    tracing::info!("agent ready — listening on 0.0.0.0:{}", config.server.listen_port);
    if let Err(e) = tunnel_server.run().await {
        tracing::error!("tunnel server error: {}", e);
    }

    // Cleanup
    let _ = shutdown_tx.send(true);
    tunnel_server.shutdown();
    if let Some(handle) = heartbeat_handle { handle.abort(); }
    router_handle.abort();

    tracing::info!("agent shutdown complete");
    Ok(())
}
