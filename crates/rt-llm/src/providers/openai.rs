//! OpenAI provider (GPT-4o, GPT-4-turbo, etc.)
//! API docs: https://platform.openai.com/docs/api-reference/chat

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
        model: "gpt-4o".to_string(),
        messages,
        max_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("OpenAI API request failed")?
        .error_for_status()
        .context("OpenAI API error status")?
        .json::<ChatResponse>()
        .await
        .context("parsing OpenAI response")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("OpenAI returned no choices"))
}
