//! Ed25519 nonce-challenge authentication for tunnel connections.
//!
//! Authentication flow (ported from C++ tunnel.cpp authenticate_client):
//! 1. Server generates 32-byte random nonce
//! 2. Server sends nonce to client
//! 3. Client signs nonce with its Ed25519 private key
//! 4. Client sends [public_key (32 bytes) || signature (64 bytes)]
//! 5. Server verifies signature against the provided public key
//! 6. Server checks public key against registered keys (via platform API or local cache)

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use std::future::Future;
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

pub const NONCE_LEN: usize = 32;
pub const PUBLIC_KEY_LEN: usize = 32;
pub const SIGNATURE_LEN: usize = 64;
const AUTH_IO_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Invalid public key")]
    InvalidPublicKey,
    #[error("Key not authorized")]
    Unauthorized,
}

/// Server-side authenticator for incoming tunnel connections.
pub struct ServerAuthenticator {
    /// Set of authorized public keys (hex-encoded).
    /// If empty, any valid signature is accepted (for development/testing).
    authorized_keys: Arc<RwLock<Vec<String>>>,
}

impl ServerAuthenticator {
    /// Create a new authenticator.
    /// If `authorized_keys` is empty, any valid Ed25519 signature is accepted.
    pub fn new(authorized_keys: Vec<String>) -> Self {
        Self {
            authorized_keys: Arc::new(RwLock::new(normalize_authorized_keys(authorized_keys))),
        }
    }

    pub fn replace_authorized_keys(&self, authorized_keys: Vec<String>) {
        if let Ok(mut guard) = self.authorized_keys.write() {
            *guard = normalize_authorized_keys(authorized_keys);
        }
    }

    pub fn authorized_keys(&self) -> Vec<String> {
        self.authorized_keys
            .read()
            .map(|keys| keys.clone())
            .unwrap_or_default()
    }

    /// Perform server-side authentication on a connected stream.
    /// Returns the hex-encoded public key of the authenticated client.
    pub async fn authenticate<S>(&self, stream: &mut S) -> Result<String, AuthError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // 1. Generate and send nonce
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        with_io_timeout(stream.write_all(&nonce), "write nonce").await?;
        with_io_timeout(stream.flush(), "flush nonce").await?;

        tracing::debug!("auth: sent nonce ({} bytes)", NONCE_LEN);

        // 2. Receive public key + signature
        let mut pub_key_bytes = [0u8; PUBLIC_KEY_LEN];
        with_io_timeout(stream.read_exact(&mut pub_key_bytes), "read public key").await?;

        let mut sig_bytes = [0u8; SIGNATURE_LEN];
        with_io_timeout(stream.read_exact(&mut sig_bytes), "read signature").await?;

        tracing::debug!("auth: received key+signature");

        // 3. Verify signature
        let verifying_key =
            VerifyingKey::from_bytes(&pub_key_bytes).map_err(|_| AuthError::InvalidPublicKey)?;
        let signature = Signature::from_bytes(&sig_bytes);

        verifying_key
            .verify(&nonce, &signature)
            .map_err(|_| AuthError::InvalidSignature)?;

        let pub_key_hex = hex::encode(pub_key_bytes);
        tracing::info!("auth: valid signature from key {}", &pub_key_hex[..16]);

        // 4. Check authorization
        let authorized_keys = self.authorized_keys();
        if !authorized_keys.is_empty() && !authorized_keys.contains(&pub_key_hex) {
            tracing::warn!("auth: key {} not in authorized list", &pub_key_hex[..16]);
            // Send rejection byte
            with_io_timeout(stream.write_u8(0x00), "write auth reject").await?;
            return Err(AuthError::Unauthorized);
        }

        // Send acceptance byte
        with_io_timeout(stream.write_u8(0x01), "write auth accept").await?;
        with_io_timeout(stream.flush(), "flush auth accept").await?;

        Ok(pub_key_hex)
    }
}

/// Client-side authentication helper.
pub struct ClientAuthenticator {
    signing_key: SigningKey,
}

impl ClientAuthenticator {
    /// Create from a 32-byte seed (private key).
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self {
            signing_key: SigningKey::from_bytes(seed),
        }
    }

    /// Perform client-side authentication on a connected stream.
    pub async fn authenticate<S>(&self, stream: &mut S) -> Result<(), AuthError>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // 1. Receive nonce
        let mut nonce = [0u8; NONCE_LEN];
        with_io_timeout(stream.read_exact(&mut nonce), "read nonce").await?;

        tracing::debug!("auth: received nonce");

        // 2. Sign nonce and send [public_key || signature]
        let signature = self.signing_key.sign(&nonce);
        let pub_key_bytes = self.signing_key.verifying_key().to_bytes();

        with_io_timeout(stream.write_all(&pub_key_bytes), "write public key").await?;
        with_io_timeout(stream.write_all(&signature.to_bytes()), "write signature").await?;
        with_io_timeout(stream.flush(), "flush auth payload").await?;

        tracing::debug!("auth: sent key+signature");

        // 3. Read acceptance/rejection
        let result = with_io_timeout(stream.read_u8(), "read auth result").await?;
        if result != 0x01 {
            return Err(AuthError::Unauthorized);
        }

        tracing::info!("auth: authenticated successfully");
        Ok(())
    }

    /// Get the hex-encoded public key.
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.signing_key.verifying_key().to_bytes())
    }
}

async fn with_io_timeout<T, F>(fut: F, op: &'static str) -> Result<T, AuthError>
where
    F: Future<Output = Result<T, std::io::Error>>,
{
    match timeout(Duration::from_secs(AUTH_IO_TIMEOUT_SECS), fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(err)) => Err(AuthError::Io(err)),
        Err(_) => Err(AuthError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("auth {} timeout", op),
        ))),
    }
}

fn hex_encode_byte(b: u8) -> [u8; 2] {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    [HEX_CHARS[(b >> 4) as usize], HEX_CHARS[(b & 0x0f) as usize]]
}

/// Simple hex encoding (avoid extra dependency).
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| {
                let h = super::hex_encode_byte(*b);
                format!("{}{}", h[0] as char, h[1] as char)
            })
            .collect()
    }
}

fn normalize_authorized_keys(keys: Vec<String>) -> Vec<String> {
    let mut normalized = keys
        .into_iter()
        .map(|key| key.trim().to_lowercase())
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_auth_success() {
        let seed: [u8; 32] = [42u8; 32];
        let client_auth = ClientAuthenticator::from_seed(&seed);
        let server_auth = ServerAuthenticator::new(vec![]); // Accept any key

        let (mut server_stream, mut client_stream) = duplex(1024);

        let server_handle =
            tokio::spawn(async move { server_auth.authenticate(&mut server_stream).await });

        let client_handle =
            tokio::spawn(async move { client_auth.authenticate(&mut client_stream).await });

        let server_result = server_handle.await.unwrap();
        let client_result = client_handle.await.unwrap();

        assert!(server_result.is_ok());
        assert!(client_result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_unauthorized_key() {
        let seed: [u8; 32] = [42u8; 32];
        let client_auth = ClientAuthenticator::from_seed(&seed);
        // Server only accepts a different key
        let server_auth = ServerAuthenticator::new(vec![
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        ]);

        let (mut server_stream, mut client_stream) = duplex(1024);

        let server_handle =
            tokio::spawn(async move { server_auth.authenticate(&mut server_stream).await });

        let client_handle =
            tokio::spawn(async move { client_auth.authenticate(&mut client_stream).await });

        let server_result = server_handle.await.unwrap();
        let client_result = client_handle.await.unwrap();

        assert!(matches!(server_result, Err(AuthError::Unauthorized)));
        assert!(matches!(client_result, Err(AuthError::Unauthorized)));
    }
}
