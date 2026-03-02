//! Async TCP tunnel server for the RoboTunnel agent.
//!
//! Handles incoming CLI connections with:
//! - Ed25519 nonce-challenge authentication
//! - Framed protocol for multiplexing tunnel packets and ZeroClaw commands
//! - Concurrent client management

use crate::auth::ServerAuthenticator;
use crate::protocol::{
    read_frame, write_frame, CommandRequest, CommandResponse, FrameType, ProtocolError,
};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing;

/// Incoming command from a connected client, to be processed by the runtime.
#[derive(Debug)]
pub struct IncomingCommand {
    pub request: CommandRequest,
    pub response_tx: mpsc::Sender<CommandResponse>,
}

/// Tunnel server that listens for CLI connections and dispatches commands.
pub struct TunnelServer {
    listen_port: u16,
    authenticator: Arc<ServerAuthenticator>,
    /// Channel for sending incoming commands to the runtime.
    command_tx: mpsc::Sender<IncomingCommand>,
    /// Broadcast channel for sending data to all connected clients (e.g., topic data).
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    /// Shutdown signal.
    shutdown: Arc<tokio::sync::Notify>,
    /// Connected client count.
    client_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl TunnelServer {
    /// Create a new tunnel server.
    pub fn new(
        listen_port: u16,
        authorized_keys: Vec<String>,
        command_tx: mpsc::Sender<IncomingCommand>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            listen_port,
            authenticator: Arc::new(ServerAuthenticator::new(authorized_keys)),
            command_tx,
            broadcast_tx,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            client_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Get the broadcast sender for publishing data to all clients.
    pub fn broadcast_tx(&self) -> broadcast::Sender<Vec<u8>> {
        self.broadcast_tx.clone()
    }

    /// Get the current number of connected clients.
    pub fn connected_clients(&self) -> usize {
        self.client_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Start the server and accept connections.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(("0.0.0.0", self.listen_port)).await?;
        tracing::info!("tunnel: listening on 0.0.0.0:{}", self.listen_port);

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            tracing::info!("tunnel: new connection from {}", addr);
                            let authenticator = self.authenticator.clone();
                            let command_tx = self.command_tx.clone();
                            let broadcast_rx = self.broadcast_tx.subscribe();
                            let client_count = self.client_count.clone();
                            let shutdown = self.shutdown.clone();

                            tokio::spawn(async move {
                                client_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if let Err(e) = handle_client(
                                    stream,
                                    authenticator,
                                    command_tx,
                                    broadcast_rx,
                                    shutdown,
                                ).await {
                                    tracing::warn!("tunnel: client {} disconnected: {}", addr, e);
                                }
                                client_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::info!("tunnel: client {} disconnected", addr);
                            });
                        }
                        Err(e) => {
                            tracing::error!("tunnel: accept error: {}", e);
                        }
                    }
                }
                _ = self.shutdown.notified() => {
                    tracing::info!("tunnel: shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// Handle a single client connection.
async fn handle_client<S>(
    mut stream: S,
    authenticator: Arc<ServerAuthenticator>,
    command_tx: mpsc::Sender<IncomingCommand>,
    mut broadcast_rx: broadcast::Receiver<Vec<u8>>,
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Authenticate
    let pub_key = authenticator
        .authenticate(&mut stream)
        .await
        .map_err(|e| ProtocolError::Io(e.to_string()))?;

    tracing::info!("tunnel: client authenticated (key: {}...)", &pub_key[..16]);

    let (mut reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(writer));

    // Writer task: forward broadcast messages to client
    let writer_clone = writer.clone();
    let write_handle = tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(data) => {
                    let mut w = writer_clone.lock().await;
                    if let Err(e) =
                        write_frame(&mut *w, FrameType::TunnelPacket, &data).await
                    {
                        tracing::debug!("tunnel: write to client failed: {}", e);
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("tunnel: client lagged by {} messages", n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Reader task: read frames from client
    loop {
        tokio::select! {
            frame_result = read_frame(&mut reader) => {
                match frame_result {
                    Ok((frame_type, data)) => {
                        match frame_type {
                            FrameType::CommandRequest => {
                                match serde_json::from_slice::<CommandRequest>(&data) {
                                    Ok(request) => {
                                        let (resp_tx, mut resp_rx) = mpsc::channel(1);
                                        let cmd = IncomingCommand {
                                            request,
                                            response_tx: resp_tx,
                                        };
                                        if command_tx.send(cmd).await.is_err() {
                                            tracing::error!("tunnel: command channel closed");
                                            break;
                                        }
                                        // Wait for response and send it back
                                        if let Some(response) = resp_rx.recv().await {
                                            let resp_data = serde_json::to_vec(&response)
                                                .unwrap_or_default();
                                            let mut w = writer.lock().await;
                                            write_frame(&mut *w, FrameType::CommandResponse, &resp_data).await?;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("tunnel: invalid command request: {}", e);
                                    }
                                }
                            }
                            FrameType::Ping => {
                                let mut w = writer.lock().await;
                                write_frame(&mut *w, FrameType::Pong, &[]).await?;
                            }
                            _ => {
                                tracing::debug!("tunnel: ignoring frame type {:?}", frame_type);
                            }
                        }
                    }
                    Err(ProtocolError::ConnectionClosed) => {
                        tracing::debug!("tunnel: client closed connection");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("tunnel: read error: {}", e);
                        break;
                    }
                }
            }
            _ = shutdown.notified() => {
                tracing::debug!("tunnel: shutdown signal received");
                break;
            }
        }
    }

    write_handle.abort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ClientAuthenticator;
    use crate::protocol::CommandStatus;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_server_accepts_connection() {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
        let server = TunnelServer::new(0, vec![], cmd_tx); // Port 0 = random

        // We need to bind first to know the port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let authenticator = server.authenticator.clone();
        let shutdown = server.shutdown.clone();

        // Spawn server handler for one connection
        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (_broadcast_tx, broadcast_rx) = broadcast::channel(16);
            handle_client(stream, authenticator, server.command_tx.clone(), broadcast_rx, shutdown)
                .await
        });

        // Connect as client
        let mut client_stream = TcpStream::connect(addr).await.unwrap();
        let client_auth = ClientAuthenticator::from_seed(&[42u8; 32]);
        client_auth.authenticate(&mut client_stream).await.unwrap();

        // Send a command request
        let req = CommandRequest {
            id: "test-1".to_string(),
            skill: "debug".to_string(),
            action: "shell".to_string(),
            params: serde_json::json!({"cmd": "echo hi"}),
        };
        let req_data = serde_json::to_vec(&req).unwrap();
        write_frame(&mut client_stream, FrameType::CommandRequest, &req_data)
            .await
            .unwrap();

        // Server should receive command
        let incoming = cmd_rx.recv().await.unwrap();
        assert_eq!(incoming.request.id, "test-1");
        assert_eq!(incoming.request.skill, "debug");

        // Send response back
        let resp = CommandResponse {
            id: "test-1".to_string(),
            status: CommandStatus::Ok,
            data: Some(serde_json::json!({"result": "done"})),
            error: None,
        };
        incoming.response_tx.send(resp).await.unwrap();

        // Client should receive response
        let (frame_type, resp_data) = read_frame(&mut client_stream).await.unwrap();
        assert_eq!(frame_type, FrameType::CommandResponse);
        let decoded: CommandResponse = serde_json::from_slice(&resp_data).unwrap();
        assert_eq!(decoded.status, CommandStatus::Ok);

        drop(client_stream);
        let _ = server_handle.await;
    }
}
