//! xAI Grok provider.
//! API docs: https://docs.x.ai/developers/model-capabilities/text/generate-text

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ResponsesRequest {
    model: String,
    input: Vec<InputMessage>,
    max_output_tokens: u32,
    store: bool,
}

#[derive(Serialize)]
struct InputMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ResponsesResponse {
    output: Vec<OutputItem>,
}

#[derive(Deserialize)]
struct OutputItem {
    #[serde(rename = "type")]
    item_type: String,
    #[serde(default)]
    content: Vec<OutputContent>,
}

#[derive(Deserialize)]
struct OutputContent {
    #[serde(rename = "type")]
    content_type: String,
    text: Option<String>,
    refusal: Option<String>,
}

pub async fn infer(api_key: &str, req: InferRequest) -> Result<String> {
    let client = reqwest::Client::new();

    let mut input = Vec::new();
    if let Some(system) = req.system {
        input.push(InputMessage {
            role: "system".into(),
            content: system,
        });
    }
    input.push(InputMessage {
        role: "user".into(),
        content: req.user,
    });

    let body = ResponsesRequest {
        model: "grok-4-fast-reasoning".to_string(),
        input,
        max_output_tokens: req.max_tokens,
        // The product promise is local-first, so opt out of xAI server-side history.
        store: false,
    };

    let resp = client
        .post("https://api.x.ai/v1/responses")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("Grok API request failed")?
        .error_for_status()
        .context("Grok API error status")?
        .json::<ResponsesResponse>()
        .await
        .context("parsing Grok response")?;

    extract_text(resp.output)
}

fn extract_text(output: Vec<OutputItem>) -> Result<String> {
    let mut text_chunks = Vec::new();
    let mut refusals = Vec::new();

    for item in output {
        if item.item_type != "message" {
            continue;
        }

        for content in item.content {
            match content.content_type.as_str() {
                "output_text" => {
                    if let Some(text) = content.text {
                        text_chunks.push(text);
                    }
                }
                "refusal" => {
                    if let Some(refusal) = content.refusal {
                        refusals.push(refusal);
                    }
                }
                _ => {}
            }
        }
    }

    if !text_chunks.is_empty() {
        return Ok(text_chunks.join("\n"));
    }

    if !refusals.is_empty() {
        return Ok(refusals.join("\n"));
    }

    anyhow::bail!("Grok returned no text output")
}

#[cfg(test)]
mod tests {
    use super::{extract_text, ResponsesResponse};

    #[test]
    fn extracts_output_text_from_responses_api_shape() {
        let payload = r#"{
          "output": [
            {
              "type": "reasoning",
              "content": []
            },
            {
              "type": "message",
              "content": [
                {
                  "type": "output_text",
                  "text": "hello from grok"
                }
              ]
            }
          ]
        }"#;

        let resp: ResponsesResponse = serde_json::from_str(payload).unwrap();
        let text = extract_text(resp.output).unwrap();
        assert_eq!(text, "hello from grok");
    }
}
