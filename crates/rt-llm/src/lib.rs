//! rt-llm — Local-encrypted LLM key storage and multi-provider inference.
//!
//! # Design
//! API keys are stored in `~/.config/robotunnel/agent.keys` as a JSON file
//! encrypted with AES-256-GCM. The encryption key is derived from the
//! machine's unique hardware ID using HKDF-SHA256. Keys **never leave
//! this machine** — LLM API calls are made directly from the agent process.
//!
//! # Supported Providers
//! OpenAI, Anthropic (Claude), Google (Gemini), xAI (Grok),
//! DeepSeek, MiniMax, Kimi (Moonshot AI), Qwen (Alibaba Cloud)

pub mod keystore;
pub mod providers;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Supported LLM provider identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    OpenAI,
    Claude,
    Gemini,
    Grok,
    DeepSeek,
    MiniMax,
    Kimi,
    Qwen,
}

impl Provider {
    /// Parse a provider from a CLI string (case-insensitive).
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "openai"   => Ok(Provider::OpenAI),
            "claude"   => Ok(Provider::Claude),
            "gemini"   => Ok(Provider::Gemini),
            "grok"     => Ok(Provider::Grok),
            "deepseek" => Ok(Provider::DeepSeek),
            "minimax"  => Ok(Provider::MiniMax),
            "kimi"     => Ok(Provider::Kimi),
            "qwen"     => Ok(Provider::Qwen),
            _ => anyhow::bail!("Unknown provider '{}'. Supported: openai, claude, gemini, grok, deepseek, minimax, kimi, qwen", s),
        }
    }

    /// Human-readable display name.
    pub fn display_name(&self) -> &'static str {
        match self {
            Provider::OpenAI   => "OpenAI (GPT-4)",
            Provider::Claude   => "Anthropic (Claude)",
            Provider::Gemini   => "Google (Gemini)",
            Provider::Grok     => "xAI (Grok)",
            Provider::DeepSeek => "DeepSeek",
            Provider::MiniMax  => "MiniMax",
            Provider::Kimi     => "Kimi (Moonshot AI)",
            Provider::Qwen     => "Qwen (Alibaba Cloud)",
        }
    }
}

/// A single inference request to an LLM.
#[derive(Debug, Clone)]
pub struct InferRequest {
    /// System prompt (optional).
    pub system: Option<String>,
    /// User message.
    pub user: String,
    /// Max tokens to generate.
    pub max_tokens: u32,
}

impl InferRequest {
    pub fn simple(user: impl Into<String>) -> Self {
        Self {
            system: None,
            user: user.into(),
            max_tokens: 1024,
        }
    }

    pub fn with_system(system: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            system: Some(system.into()),
            user: user.into(),
            max_tokens: 1024,
        }
    }
}

/// Manager that ties together key storage and provider dispatch.
pub struct LlmManager {
    keystore: keystore::KeyStore,
}

impl LlmManager {
    /// Open (or create) the key store using the machine-derived encryption key.
    pub fn open() -> Result<Self> {
        let keystore = keystore::KeyStore::open()?;
        Ok(Self { keystore })
    }

    /// Store an API key for a provider.
    pub fn set_key(&mut self, provider: &Provider, api_key: &str) -> Result<()> {
        self.keystore.set(provider, api_key)?;
        tracing::info!("API key set for {}", provider.display_name());
        Ok(())
    }

    /// Remove an API key.
    pub fn remove_key(&mut self, provider: &Provider) -> Result<bool> {
        let removed = self.keystore.remove(provider)?;
        if removed {
            tracing::info!("API key removed for {}", provider.display_name());
        }
        Ok(removed)
    }

    /// List all configured providers and their masked keys.
    pub fn list_keys(&self) -> Vec<(Provider, String)> {
        self.keystore.list()
    }

    /// Run inference using the specified provider.
    /// Returns the generated text response.
    pub async fn infer(&self, provider: &Provider, req: InferRequest) -> Result<String> {
        let api_key = self.keystore
            .get(provider)?
            .ok_or_else(|| anyhow::anyhow!(
                "No API key configured for {}. Run: robotunnel-agent keys set {} <api-key>",
                provider.display_name(),
                format!("{:?}", provider).to_lowercase()
            ))?;

        match provider {
            Provider::OpenAI   => providers::openai::infer(&api_key, req).await,
            Provider::Claude   => providers::claude::infer(&api_key, req).await,
            Provider::Gemini   => providers::gemini::infer(&api_key, req).await,
            Provider::Grok     => providers::grok::infer(&api_key, req).await,
            Provider::DeepSeek => providers::deepseek::infer(&api_key, req).await,
            Provider::MiniMax  => providers::minimax::infer(&api_key, req).await,
            Provider::Kimi     => providers::kimi::infer(&api_key, req).await,
            Provider::Qwen     => providers::qwen::infer(&api_key, req).await,
        }
    }
}
