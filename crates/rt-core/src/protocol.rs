//! Wire protocol definitions for RoboTunnel tunnel communication.
//!
//! Packet format (binary, big-endian):
//! ```text
//! [topic_len: u16][topic: bytes]
//! [type_len:  u16][type:  bytes]
//! [timestamp_ns: u64]
//! [payload_len: u32][payload: bytes]
//! ```

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("Invalid packet: {0}")]
    InvalidPacket(String),
    #[error("Connection closed")]
    ConnectionClosed,
}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            ProtocolError::ConnectionClosed
        } else {
            ProtocolError::Io(e.to_string())
        }
    }
}

/// A tunnel packet carrying topic data (legacy v0.1 format, used for ROS2 topic proxy).
#[derive(Debug, Clone)]
pub struct TunnelPacket {
    pub topic: String,
    pub msg_type: String,
    pub timestamp_ns: u64,
    pub payload: Vec<u8>,
}

impl TunnelPacket {
    /// Write this packet to an async writer.
    pub async fn write_to<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
    ) -> Result<(), ProtocolError> {
        let topic_bytes = self.topic.as_bytes();
        let type_bytes = self.msg_type.as_bytes();

        writer.write_u16(topic_bytes.len() as u16).await?;
        writer.write_all(topic_bytes).await?;
        writer.write_u16(type_bytes.len() as u16).await?;
        writer.write_all(type_bytes).await?;
        writer.write_u64(self.timestamp_ns).await?;
        writer.write_u32(self.payload.len() as u32).await?;
        writer.write_all(&self.payload).await?;
        writer.flush().await?;

        Ok(())
    }

    /// Read a packet from an async reader.
    pub async fn read_from<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self, ProtocolError> {
        let topic_len = reader.read_u16().await? as usize;
        if topic_len > 4096 {
            return Err(ProtocolError::InvalidPacket("topic too long".into()));
        }
        let mut topic_buf = vec![0u8; topic_len];
        reader.read_exact(&mut topic_buf).await?;

        let type_len = reader.read_u16().await? as usize;
        if type_len > 4096 {
            return Err(ProtocolError::InvalidPacket("type too long".into()));
        }
        let mut type_buf = vec![0u8; type_len];
        reader.read_exact(&mut type_buf).await?;

        let timestamp_ns = reader.read_u64().await?;

        let payload_len = reader.read_u32().await? as usize;
        if payload_len > 64 * 1024 * 1024 {
            return Err(ProtocolError::InvalidPacket("payload too large".into()));
        }
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;

        Ok(TunnelPacket {
            topic: String::from_utf8_lossy(&topic_buf).into_owned(),
            msg_type: String::from_utf8_lossy(&type_buf).into_owned(),
            timestamp_ns,
            payload,
        })
    }
}

/// A RoboTunnel command request sent from platform to agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequest {
    pub id: String,
    pub skill: String,
    pub action: String,
    pub params: serde_json::Value,
}

/// A RoboTunnel command response sent from agent to platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub id: String,
    pub status: CommandStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// JSON payload for WebRtcBootstrap frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebRtcBootstrapPayload {
    pub bootstrap_id: String,
    pub cli_public_ip: Option<String>,
    pub cli_lan_cidr: Option<String>,
}

/// JSON payload for WebRtcTeardown frame.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebRtcTeardownPayload {
    #[serde(default)]
    pub bootstrap_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CommandStatus {
    Ok,
    Error,
    Timeout,
}

/// Frame types for the v0.2 protocol multiplexing.
/// The first byte of each frame indicates its type.
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum FrameType {
    /// Legacy tunnel packet (ROS2 topic data)
    TunnelPacket = 0x01,
    /// RoboTunnel command request (JSON)
    CommandRequest = 0x02,
    /// RoboTunnel command response (JSON)
    CommandResponse = 0x03,
    /// Ping/pong for keepalive
    Ping = 0x10,
    Pong = 0x11,
    /// Trigger WebRTC bootstrap (Platform -> Agent)
    WebRtcBootstrap = 0x20,
    /// Terminate WebRTC resources (Platform -> Agent)
    WebRtcTeardown = 0x21,
}

impl TryFrom<u8> for FrameType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(FrameType::TunnelPacket),
            0x02 => Ok(FrameType::CommandRequest),
            0x03 => Ok(FrameType::CommandResponse),
            0x10 => Ok(FrameType::Ping),
            0x11 => Ok(FrameType::Pong),
            0x20 => Ok(FrameType::WebRtcBootstrap),
            0x21 => Ok(FrameType::WebRtcTeardown),
            _ => Err(ProtocolError::InvalidPacket(format!(
                "unknown frame type: 0x{:02x}",
                value
            ))),
        }
    }
}

/// Write a framed message: [frame_type: u8][length: u32][data: bytes]
pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    frame_type: FrameType,
    data: &[u8],
) -> Result<(), ProtocolError> {
    writer.write_u8(frame_type as u8).await?;
    writer.write_u32(data.len() as u32).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a framed message: returns (frame_type, data).
pub async fn read_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<(FrameType, Vec<u8>), ProtocolError> {
    let type_byte = reader.read_u8().await?;
    let frame_type = FrameType::try_from(type_byte)?;
    let length = reader.read_u32().await? as usize;

    if length > 64 * 1024 * 1024 {
        return Err(ProtocolError::InvalidPacket("frame too large".into()));
    }

    let mut data = vec![0u8; length];
    reader.read_exact(&mut data).await?;
    Ok((frame_type, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_tunnel_packet_roundtrip() {
        let packet = TunnelPacket {
            topic: "/lidar/points".to_string(),
            msg_type: "sensor_msgs/msg/PointCloud2".to_string(),
            timestamp_ns: 1234567890,
            payload: vec![1, 2, 3, 4, 5],
        };

        let mut buf = Vec::new();
        packet.write_to(&mut buf).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded = TunnelPacket::read_from(&mut cursor).await.unwrap();

        assert_eq!(decoded.topic, packet.topic);
        assert_eq!(decoded.msg_type, packet.msg_type);
        assert_eq!(decoded.timestamp_ns, packet.timestamp_ns);
        assert_eq!(decoded.payload, packet.payload);
    }

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let data = b"hello world";
        let mut buf = Vec::new();
        write_frame(&mut buf, FrameType::CommandRequest, data)
            .await
            .unwrap();

        let mut cursor = Cursor::new(buf);
        let (frame_type, decoded_data) = read_frame(&mut cursor).await.unwrap();

        assert_eq!(frame_type, FrameType::CommandRequest);
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn test_command_request_serialization() {
        let req = CommandRequest {
            id: "test-123".to_string(),
            skill: "debug".to_string(),
            action: "shell".to_string(),
            params: serde_json::json!({"cmd": "uptime"}),
        };

        let json = serde_json::to_string(&req).unwrap();
        let decoded: CommandRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, "test-123");
        assert_eq!(decoded.skill, "debug");
    }

    #[test]
    fn test_command_response_serialization() {
        let resp = CommandResponse {
            id: "test-123".to_string(),
            status: CommandStatus::Ok,
            data: Some(serde_json::json!({"stdout": "hello"})),
            error: None,
        };

        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("error"));

        let decoded: CommandResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.status, CommandStatus::Ok);
    }
}
