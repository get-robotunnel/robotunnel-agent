use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLocalBuffer {
    pub desired_delay_ms: u64,
    pub max_buffer_ms: u64,
}

impl SessionLocalBuffer {
    pub fn new(desired_delay_ms: u64) -> Self {
        let clamped = desired_delay_ms.clamp(0, 5000);
        Self {
            desired_delay_ms: clamped,
            max_buffer_ms: (clamped + 300).min(6000),
        }
    }
}
