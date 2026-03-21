//! Kimi (Moonshot AI) provider.
//! API docs: https://platform.moonshot.cn/docs/api/chat (OpenAI-compatible)

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const DEFAULT_KIMI_MODEL: &str = "moonshot-v1-8k";

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<Message>,
    max_tokens: u32,
}

#[derive(Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: MessageContent,
}

#[derive(Deserialize)]
struct MessageContent {
    content: String,
}

fn kimi_model() -> String {
    resolve_kimi_model(std::env::var("RT_LLM_KIMI_MODEL").ok())
}

fn resolve_kimi_model(override_value: Option<String>) -> String {
    override_value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_KIMI_MODEL.to_string())
}

pub async fn infer(api_key: &str, req: InferRequest) -> Result<String> {
    let client = reqwest::Client::new();

    let mut messages = Vec::new();
    if let Some(system) = req.system {
        messages.push(Message {
            role: "system".into(),
            content: system,
        });
    }
    messages.push(Message {
        role: "user".into(),
        content: req.user,
    });

    let body = ChatRequest {
        model: kimi_model(),
        messages,
        max_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://api.moonshot.cn/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("Kimi API request failed")?
        .error_for_status()
        .context("Kimi API error status")?
        .json::<ChatResponse>()
        .await
        .context("parsing Kimi response")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("Kimi returned no choices"))
}

#[cfg(test)]
mod tests {
    use super::{resolve_kimi_model, DEFAULT_KIMI_MODEL};

    #[test]
    fn kimi_model_defaults_to_stable_supported_model() {
        assert_eq!(resolve_kimi_model(None), DEFAULT_KIMI_MODEL);
    }

    #[test]
    fn kimi_model_honors_override() {
        assert_eq!(
            resolve_kimi_model(Some("kimi-k2-turbo-preview".to_string())),
            "kimi-k2-turbo-preview"
        );
    }
}
