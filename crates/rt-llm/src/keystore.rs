//! Encrypted local key store for LLM API keys.
//!
//! Storage format: `~/.config/robotunnel/agent.keys`
//! Encryption: AES-256-GCM with a key derived via HKDF-SHA256 from the
//! machine's hardware ID. The nonce is stored prepended to the ciphertext.
//!
//! The machine ID on Linux is read from `/etc/machine-id`.
//! On macOS (dev only) we fall back to hostname + username.

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{collections::HashMap, fs, path::PathBuf};

use crate::Provider;

/// In-memory representation of the key store (decrypted).
#[derive(Debug, Default, Serialize, Deserialize)]
struct KeyMap {
    keys: HashMap<String, String>,
}

/// Manages encrypted storage and retrieval of LLM API keys.
pub struct KeyStore {
    path: PathBuf,
    enc_key: [u8; 32],
    map: KeyMap,
}

impl KeyStore {
    /// Open the key store. Creates directory and empty store if first run.
    pub fn open() -> Result<Self> {
        let path = Self::store_path()?;
        let enc_key = derive_encryption_key()?;

        let map = if path.exists() {
            let ciphertext =
                fs::read(&path).with_context(|| format!("reading key store at {:?}", path))?;
            decrypt_map(&enc_key, &ciphertext)?
        } else {
            // First run — create parent dir, empty map
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating config dir {:?}", parent))?;
            }
            KeyMap::default()
        };

        Ok(Self { path, enc_key, map })
    }

    /// Store an API key, then persist to disk (encrypted).
    pub fn set(&mut self, provider: &Provider, api_key: &str) -> Result<()> {
        if api_key.trim().is_empty() {
            bail!("API key cannot be empty");
        }
        let key_name = provider_key(provider);
        self.map.keys.insert(key_name, api_key.to_string());
        self.flush()
    }

    /// Retrieve an API key, or None if not set.
    pub fn get(&self, provider: &Provider) -> Result<Option<String>> {
        Ok(self.map.keys.get(&provider_key(provider)).cloned())
    }

    /// Remove a key. Returns true if it was present.
    pub fn remove(&mut self, provider: &Provider) -> Result<bool> {
        let existed = self.map.keys.remove(&provider_key(provider)).is_some();
        if existed {
            self.flush()?;
        }
        Ok(existed)
    }

    /// List all configured providers with masked API keys.
    pub fn list(&self) -> Vec<(Provider, String)> {
        let all_providers = [
            Provider::OpenAI,
            Provider::Claude,
            Provider::Gemini,
            Provider::Grok,
            Provider::DeepSeek,
            Provider::MiniMax,
            Provider::Kimi,
            Provider::Qwen,
        ];
        all_providers
            .into_iter()
            .filter_map(|p| {
                self.map.keys.get(&provider_key(&p)).map(|k| {
                    let masked = mask_key(k);
                    (p, masked)
                })
            })
            .collect()
    }

    /// Write the current map to disk (encrypted).
    fn flush(&self) -> Result<()> {
        let ciphertext = encrypt_map(&self.enc_key, &self.map)?;
        fs::write(&self.path, &ciphertext)
            .with_context(|| format!("writing key store to {:?}", self.path))?;
        Ok(())
    }

    fn store_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("~/.config"));
        Ok(config_dir.join("robotunnel").join("agent.keys"))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn provider_key(provider: &Provider) -> String {
    format!("{:?}", provider).to_lowercase()
}

fn mask_key(key: &str) -> String {
    let len = key.len();
    if len <= 8 {
        return "*".repeat(len);
    }
    let visible_prefix = 4.min(len / 4);
    let visible_suffix = 4.min(len / 4);
    format!(
        "{}...{}",
        &key[..visible_prefix],
        &key[len - visible_suffix..]
    )
}

/// Derive a 32-byte encryption key from the machine's hardware ID.
/// Uses HKDF-SHA256 with a fixed info string for domain separation.
fn derive_encryption_key() -> Result<[u8; 32]> {
    let machine_id = read_machine_id()?;
    let hk = Hkdf::<Sha256>::new(Some(b"robotunnel-keystore-v1"), machine_id.as_bytes());
    let mut okm = [0u8; 32];
    hk.expand(b"llm-api-key-encryption", &mut okm)
        .map_err(|_| anyhow::anyhow!("HKDF expand failed"))?;
    Ok(okm)
}

/// Read a unique machine identifier.
/// - Linux: `/etc/machine-id` (systemd)
/// - macOS / fallback: concatenate hostname + username (dev mode only)
fn read_machine_id() -> Result<String> {
    // Linux (primary deployment target)
    if let Ok(id) = fs::read_to_string("/etc/machine-id") {
        let trimmed = id.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }

    // macOS / other (development only)
    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown-host".to_string());
    let username = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown-user".to_string());

    Ok(format!("{}-{}", hostname, username))
}

/// Encrypt the key map. Returns `nonce || ciphertext`, base64-encoded for
/// safe file storage.
fn encrypt_map(enc_key: &[u8; 32], map: &KeyMap) -> Result<Vec<u8>> {
    let plaintext = serde_json::to_vec(map).context("serializing key map")?;
    let key = Key::<Aes256Gcm>::from_slice(enc_key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_slice())
        .map_err(|_| anyhow::anyhow!("AES-GCM encryption failed"))?;

    // Format: base64(nonce) + "." + base64(ciphertext)
    let encoded = format!(
        "{}.{}",
        BASE64.encode(nonce.as_slice()),
        BASE64.encode(&ciphertext)
    );
    Ok(encoded.into_bytes())
}

/// Decrypt the key map from the on-disk format.
fn decrypt_map(enc_key: &[u8; 32], raw: &[u8]) -> Result<KeyMap> {
    let content = std::str::from_utf8(raw).context("key store is not valid UTF-8")?;
    let parts: Vec<&str> = content.splitn(2, '.').collect();
    if parts.len() != 2 {
        bail!("Key store format invalid — may be from an older version or corrupted");
    }
    let nonce_bytes = BASE64.decode(parts[0]).context("decoding nonce")?;
    let ciphertext = BASE64.decode(parts[1]).context("decoding ciphertext")?;

    let key = Key::<Aes256Gcm>::from_slice(enc_key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plaintext = cipher.decrypt(nonce, ciphertext.as_slice()).map_err(|_| {
        anyhow::anyhow!(
            "Key store decryption failed. This usually means the file was created on \
             a different machine. Delete {:?} to reset.",
            dirs::config_dir()
                .unwrap_or_default()
                .join("robotunnel/agent.keys")
        )
    })?;

    serde_json::from_slice(&plaintext).context("parsing decrypted key map")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_key() {
        assert_eq!(mask_key("sk-abcdefghijklmnop"), "sk-a...mnop");
        assert_eq!(mask_key("short"), "*****");
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [42u8; 32];
        let map = KeyMap {
            keys: {
                let mut m = HashMap::new();
                m.insert("openai".to_string(), "sk-test-1234".to_string());
                m
            },
        };
        let encrypted = encrypt_map(&key, &map).unwrap();
        let decrypted = decrypt_map(&key, &encrypted).unwrap();
        assert_eq!(decrypted.keys.get("openai").unwrap(), "sk-test-1234");
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];
        let map = KeyMap::default();
        let encrypted = encrypt_map(&key1, &map).unwrap();
        assert!(decrypt_map(&key2, &encrypted).is_err());
    }
}
