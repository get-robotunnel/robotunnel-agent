//! LLM provider implementations.
//!
//! Each module exposes a single `infer(api_key, req) -> Result<String>` function.
//! All API calls are made directly from the agent — the Platform Gateway is
//! NOT in the path for inference.

pub mod claude;
pub mod deepseek;
pub mod gemini;
pub mod grok;
pub mod kimi;
pub mod minimax;
pub mod openai;
pub mod qwen;
