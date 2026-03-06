//! xAI Grok provider.
//! API docs: https://docs.x.ai/api (OpenAI-compatible)

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

// Grok uses OpenAI-compatible API — same structs, different base URL and model.
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
        model: "grok-3".to_string(),
        messages,
        max_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://api.x.ai/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("Grok API request failed")?
        .error_for_status()
        .context("Grok API error status")?
        .json::<ChatResponse>()
        .await
        .context("parsing Grok response")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("Grok returned no choices"))
}
