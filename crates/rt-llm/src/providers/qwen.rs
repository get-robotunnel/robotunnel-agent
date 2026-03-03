//! Qwen (Alibaba Cloud) provider.
//! API docs: https://help.aliyun.com/zh/model-studio/developer-reference/use-qwen-by-calling-api
//! Uses OpenAI-compatible endpoint.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use crate::InferRequest;

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
        messages.push(Message { role: "system".into(), content: system });
    }
    messages.push(Message { role: "user".into(), content: req.user });

    let body = ChatRequest {
        model: "qwen-plus".to_string(),
        messages,
        max_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("Qwen API request failed")?
        .error_for_status()
        .context("Qwen API error status")?
        .json::<ChatResponse>()
        .await
        .context("parsing Qwen response")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("Qwen returned no choices"))
}
