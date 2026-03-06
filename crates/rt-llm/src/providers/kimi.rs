//! Kimi (Moonshot AI) provider.
//! API docs: https://platform.moonshot.cn/docs/api/chat (OpenAI-compatible)

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
        model: "moonshot-v1-8k".to_string(),
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
