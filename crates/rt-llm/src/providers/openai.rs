//! OpenAI provider.
//! API docs: https://platform.openai.com/docs/api-reference/responses

use crate::InferRequest;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ResponsesRequest {
    model: String,
    input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    max_output_tokens: u32,
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

    let body = ResponsesRequest {
        model: "gpt-4o".to_string(),
        input: req.user,
        instructions: req.system,
        max_output_tokens: req.max_tokens,
    };

    let resp = client
        .post("https://api.openai.com/v1/responses")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("OpenAI API request failed")?
        .error_for_status()
        .context("OpenAI API error status")?
        .json::<ResponsesResponse>()
        .await
        .context("parsing OpenAI response")?;

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

    anyhow::bail!("OpenAI returned no text output")
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
              "role": "assistant",
              "content": [
                {
                  "type": "output_text",
                  "text": "hello from responses"
                }
              ]
            }
          ]
        }"#;

        let resp: ResponsesResponse = serde_json::from_str(payload).unwrap();
        let text = extract_text(resp.output).unwrap();
        assert_eq!(text, "hello from responses");
    }

    #[test]
    fn falls_back_to_refusal_text() {
        let payload = r#"{
          "output": [
            {
              "type": "message",
              "role": "assistant",
              "content": [
                {
                  "type": "refusal",
                  "refusal": "cannot comply"
                }
              ]
            }
          ]
        }"#;

        let resp: ResponsesResponse = serde_json::from_str(payload).unwrap();
        let text = extract_text(resp.output).unwrap();
        assert_eq!(text, "cannot comply");
    }
}
