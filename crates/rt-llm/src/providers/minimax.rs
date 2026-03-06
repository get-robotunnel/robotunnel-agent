//! MiniMax provider.
//! API docs: https://platform.minimaxi.com/document/ChatCompletion%20v2

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
    name: Option<String>,
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

    let mut messages: Vec<Message> = Vec::new();
    if let Some(system) = &req.system {
        // MiniMax uses a "system" role with a name field
        messages.push(Message {
            role: "system".into(),
            content: system.clone(),
            name: Some("MM智能助理".into()),
        });
    }
    messages.push(Message {
        role: "user".into(),
        content: req.user,
        name: None,
    });

    let body = ChatRequest {
        model: "MiniMax-Text-01".to_string(),
        messages,
        max_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://api.minimaxi.chat/v1/text/chatcompletion_v2")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("MiniMax API request failed")?
        .error_for_status()
        .context("MiniMax API error status")?
        .json::<ChatResponse>()
        .await
        .context("parsing MiniMax response")?;

    resp.choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("MiniMax returned no choices"))
}
