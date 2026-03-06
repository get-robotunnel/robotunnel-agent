//! Google Gemini provider.
//! API docs: https://ai.google.dev/api/generate-content

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct GenerateRequest {
    contents: Vec<Content>,
    #[serde(rename = "generationConfig")]
    generation_config: GenerationConfig,
}

#[derive(Serialize)]
struct Content {
    parts: Vec<Part>,
    role: String,
}

#[derive(Serialize)]
struct Part {
    text: String,
}

#[derive(Serialize)]
struct GenerationConfig {
    #[serde(rename = "maxOutputTokens")]
    max_output_tokens: u32,
}

#[derive(Deserialize)]
struct GenerateResponse {
    candidates: Vec<Candidate>,
}

#[derive(Deserialize)]
struct Candidate {
    content: ContentResponse,
}

#[derive(Deserialize)]
struct ContentResponse {
    parts: Vec<PartResponse>,
}

#[derive(Deserialize)]
struct PartResponse {
    text: String,
}

pub async fn infer(api_key: &str, req: InferRequest) -> Result<String> {
    let client = reqwest::Client::new();
    let model = "gemini-2.0-flash";
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        model, api_key
    );

    let mut contents = Vec::new();
    // Gemini uses "user" role for user messages; system instructions go in system_instruction field
    // For simplicity, prepend system prompt as a user message if present
    if let Some(system) = &req.system {
        contents.push(Content {
            role: "user".into(),
            parts: vec![Part {
                text: format!("[System]: {}", system),
            }],
        });
        contents.push(Content {
            role: "model".into(),
            parts: vec![Part {
                text: "Understood.".into(),
            }],
        });
    }
    contents.push(Content {
        role: "user".into(),
        parts: vec![Part { text: req.user }],
    });

    let body = GenerateRequest {
        contents,
        generation_config: GenerationConfig {
            max_output_tokens: req.max_tokens,
        },
    };

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .context("Gemini API request failed")?
        .error_for_status()
        .context("Gemini API error status")?
        .json::<GenerateResponse>()
        .await
        .context("parsing Gemini response")?;

    resp.candidates
        .into_iter()
        .next()
        .and_then(|c| c.content.parts.into_iter().next())
        .map(|p| p.text)
        .ok_or_else(|| anyhow::anyhow!("Gemini returned no content"))
}
