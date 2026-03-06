//! Anthropic (Claude) provider.
//! API docs: https://docs.anthropic.com/en/api/messages

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct MessagesRequest {
    model: String,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    messages: Vec<Message>,
}

#[derive(Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
}

pub async fn infer(api_key: &str, req: InferRequest) -> Result<String> {
    let client = reqwest::Client::new();

    let body = MessagesRequest {
        model: "claude-opus-4-5".to_string(),
        max_tokens: req.max_tokens,
        system: req.system,
        messages: vec![Message {
            role: "user".into(),
            content: req.user,
        }],
    };

    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .json(&body)
        .send()
        .await
        .context("Claude API request failed")?
        .error_for_status()
        .context("Claude API error status")?
        .json::<MessagesResponse>()
        .await
        .context("parsing Claude response")?;

    resp.content
        .into_iter()
        .find(|b| b.block_type == "text")
        .and_then(|b| b.text)
        .ok_or_else(|| anyhow::anyhow!("Claude returned no text content"))
}
