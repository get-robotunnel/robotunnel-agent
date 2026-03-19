use serde::Serialize;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone, Serialize)]
pub struct TopicStats {
    pub topic: String,
    pub window_sec: u64,
    pub average_hz: Option<f64>,
    pub average_bw: Option<String>,
    pub average_delay_sec: Option<f64>,
    pub raw_hz: String,
    pub raw_bw: String,
    pub raw_delay: String,
}

pub async fn collect_topic_stats(topic: &str, window_sec: u64) -> Result<TopicStats, String> {
    let topic = topic.trim();
    if topic.is_empty() {
        return Err("missing required param: topic".to_string());
    }
    let window_sec = window_sec.clamp(3, 60);

    let hz_cmd = format!(
        "timeout {}s ros2 topic hz '{}' 2>&1 || true",
        window_sec,
        shell_quote(topic)
    );
    let bw_cmd = format!(
        "timeout {}s ros2 topic bw '{}' 2>&1 || true",
        window_sec,
        shell_quote(topic)
    );
    let delay_cmd = format!(
        "timeout {}s ros2 topic delay '{}' 2>&1 || true",
        window_sec,
        shell_quote(topic)
    );

    // Run collectors in parallel so end-to-end latency is ~window_sec
    // instead of three sequential windows.
    let (hz_res, bw_res, delay_res) = tokio::join!(
        run_shell(&hz_cmd, window_sec + 3),
        run_shell(&bw_cmd, window_sec + 3),
        run_shell(&delay_cmd, window_sec + 3),
    );

    let hz_out = hz_res.unwrap_or_else(|err| format!("collector_error: {}", err));
    let bw_out = bw_res.unwrap_or_else(|err| format!("collector_error: {}", err));
    let delay_out = delay_res.unwrap_or_else(|err| format!("collector_error: {}", err));

    Ok(TopicStats {
        topic: topic.to_string(),
        window_sec,
        average_hz: parse_metric_value(&hz_out, "average rate:"),
        average_bw: parse_bw_value(&bw_out),
        average_delay_sec: parse_metric_value(&delay_out, "average delay:"),
        raw_hz: hz_out.trim().to_string(),
        raw_bw: bw_out.trim().to_string(),
        raw_delay: delay_out.trim().to_string(),
    })
}

async fn run_shell(script: &str, timeout_sec: u64) -> Result<String, String> {
    let mut cmd = Command::new("sh");
    cmd.args(["-lc", script]);

    let duration = Duration::from_secs(timeout_sec.clamp(2, 120));
    let output = timeout(duration, cmd.output())
        .await
        .map_err(|_| format!("command timed out after {}s", duration.as_secs()))?
        .map_err(|err| err.to_string())?;

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn parse_metric_value(raw: &str, key: &str) -> Option<f64> {
    raw.lines().find_map(|line| {
        let line = line.trim();
        let idx = line.to_lowercase().find(&key.to_lowercase())?;
        let tail = line[idx + key.len()..].trim();
        let token = tail
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(',');
        token.parse::<f64>().ok()
    })
}

fn parse_bw_value(raw: &str) -> Option<String> {
    raw.lines().find_map(|line| {
        let line = line.trim();
        if !line.to_lowercase().contains("average:") {
            return None;
        }
        let tail = line.split(':').nth(1)?.trim();
        if tail.is_empty() {
            None
        } else {
            Some(tail.to_string())
        }
    })
}

fn shell_quote(value: &str) -> String {
    value.replace('\'', "'\\''")
}

#[cfg(test)]
mod tests {
    use super::{parse_bw_value, parse_metric_value, shell_quote};

    #[test]
    fn parse_metric_value_extracts_average() {
        let raw = "average rate: 12.345\nmin: 1.0";
        assert_eq!(parse_metric_value(raw, "average rate:"), Some(12.345));
    }

    #[test]
    fn parse_metric_value_returns_none_on_sparse_output() {
        let raw = "no new messages";
        assert_eq!(parse_metric_value(raw, "average rate:"), None);
        assert_eq!(parse_metric_value(raw, "average delay:"), None);
    }

    #[test]
    fn parse_bw_value_extracts_human_readable_bandwidth() {
        let raw = "average: 15.63KB/s";
        assert_eq!(parse_bw_value(raw), Some("15.63KB/s".to_string()));
    }

    #[test]
    fn shell_quote_escapes_single_quote() {
        let quoted = shell_quote("/tmp/te'st");
        assert_eq!(quoted, "/tmp/te'\\''st");
    }
}
