use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    pub seq: u64,
    pub topic: String,
    pub topic_type: Option<String>,
    pub captured_at_unix: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub input_points: Option<u64>,
    pub output_points: Option<u64>,
    pub input_width_px: Option<u32>,
    pub input_height_px: Option<u32>,
    pub output_width_px: Option<u32>,
    pub output_height_px: Option<u32>,
    pub input_preview: Option<String>,
    pub output_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStreamBuffer {
    pub max_messages: usize,
    pub dropped_messages: u64,
    pub last_seq: u64,
    pub messages: VecDeque<StreamMessage>,
}

impl TopicStreamBuffer {
    pub fn new(max_messages: usize) -> Self {
        Self {
            max_messages: max_messages.max(1),
            dropped_messages: 0,
            last_seq: 0,
            messages: VecDeque::new(),
        }
    }

    pub fn push(&mut self, mut message: StreamMessage) {
        self.last_seq = self.last_seq.saturating_add(1);
        message.seq = self.last_seq;

        if self.messages.len() >= self.max_messages {
            self.messages.pop_front();
            self.dropped_messages = self.dropped_messages.saturating_add(1);
        }
        self.messages.push_back(message);
    }

    pub fn pull(&self, since_seq: Option<u64>, limit: usize) -> Vec<StreamMessage> {
        let limit = limit.max(1).min(self.max_messages);
        let since = since_seq.unwrap_or(0);
        self.messages
            .iter()
            .filter(|msg| msg.seq > since)
            .take(limit)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLocalBuffer {
    pub desired_delay_ms: u64,
    pub max_buffer_ms: u64,
    pub max_messages_per_topic: usize,
    pub topics: BTreeMap<String, TopicStreamBuffer>,
}

impl SessionLocalBuffer {
    pub fn new(desired_delay_ms: u64) -> Self {
        let clamped = desired_delay_ms.clamp(0, 5000);
        Self {
            desired_delay_ms: clamped,
            max_buffer_ms: (clamped + 300).min(6000),
            max_messages_per_topic: 32,
            topics: BTreeMap::new(),
        }
    }

    pub fn ensure_topic(&mut self, topic: &str) {
        let topic = topic.trim();
        if topic.is_empty() {
            return;
        }
        self.topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicStreamBuffer::new(self.max_messages_per_topic));
    }

    pub fn push_message(&mut self, topic: &str, message: StreamMessage) {
        let topic = topic.trim();
        if topic.is_empty() {
            return;
        }
        let entry = self
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicStreamBuffer::new(self.max_messages_per_topic));
        entry.push(message);
    }

    pub fn pull_messages(
        &self,
        topic: &str,
        since_seq: Option<u64>,
        limit: usize,
    ) -> Option<Vec<StreamMessage>> {
        let topic = topic.trim();
        if topic.is_empty() {
            return None;
        }
        let entry = self.topics.get(topic)?;
        Some(entry.pull(since_seq, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_stream_buffer_assigns_seq_and_drops_oldest() {
        let mut buf = TopicStreamBuffer::new(2);
        for i in 0..3 {
            buf.push(StreamMessage {
                seq: 0,
                topic: "/scan".to_string(),
                topic_type: None,
                captured_at_unix: i,
                input_bytes: 10,
                output_bytes: 5,
                input_points: None,
                output_points: None,
                input_width_px: None,
                input_height_px: None,
                output_width_px: None,
                output_height_px: None,
                input_preview: None,
                output_preview: None,
            });
        }
        assert_eq!(buf.last_seq, 3);
        assert_eq!(buf.messages.len(), 2);
        assert_eq!(buf.dropped_messages, 1);
        assert_eq!(buf.messages.front().map(|m| m.seq), Some(2));
    }

    #[test]
    fn session_local_buffer_pull_respects_since_seq() {
        let mut session = SessionLocalBuffer::new(100);
        session.push_message(
            "/image",
            StreamMessage {
                seq: 0,
                topic: "/image".to_string(),
                topic_type: None,
                captured_at_unix: 1,
                input_bytes: 100,
                output_bytes: 30,
                input_points: None,
                output_points: None,
                input_width_px: Some(640),
                input_height_px: Some(480),
                output_width_px: Some(320),
                output_height_px: Some(240),
                input_preview: Some("a".to_string()),
                output_preview: Some("b".to_string()),
            },
        );
        session.push_message(
            "/image",
            StreamMessage {
                seq: 0,
                topic: "/image".to_string(),
                topic_type: None,
                captured_at_unix: 2,
                input_bytes: 100,
                output_bytes: 30,
                input_points: None,
                output_points: None,
                input_width_px: Some(640),
                input_height_px: Some(480),
                output_width_px: Some(320),
                output_height_px: Some(240),
                input_preview: Some("c".to_string()),
                output_preview: Some("d".to_string()),
            },
        );
        let pulled = session
            .pull_messages("/image", Some(1), 8)
            .expect("buffer should exist");
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].seq, 2);
    }
}
