use async_trait::async_trait;
use rt_agent_dispatch::{ExecutionResult, Skill, SkillError};
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

pub struct Ros2Skill {
    bridge_url: String,
}

impl Ros2Skill {
    pub fn new(bridge_url: &str) -> Self {
        Self {
            bridge_url: bridge_url.to_string(),
        }
    }
}

#[async_trait]
impl Skill for Ros2Skill {
    fn name(&self) -> &str {
        "ros2_observe"
    }

    async fn execute(
        &self,
        action: &str,
        params: Value,
        _broadcast_tx: broadcast::Sender<Vec<u8>>,
    ) -> ExecutionResult {
        match action {
            "list_topics" => list_topics(self).await,
            "topic_info" => topic_info(self, &params).await,
            "subscribe" => subscribe_samples(self, &params).await,
            "topic_stats" => topic_stats(self, &params).await,
            "stream_endpoint" => stream_endpoint(self, &params).await,
            _ => Err(SkillError::ActionNotFound(action.to_string())),
        }
    }
}

async fn list_topics(skill: &Ros2Skill) -> ExecutionResult {
    let out = run_ros2(&["topic", "list", "-t"], 6).await?;
    let topics = parse_topics_with_types(&out.stdout);
    Ok(json!({
        "source": "ros2_cli",
        "bridge_url": skill.bridge_url,
        "count": topics.len(),
        "topics": topics,
    }))
}

async fn topic_info(skill: &Ros2Skill, params: &Value) -> ExecutionResult {
    let topic = required_topic(params)?;
    let info = run_ros2(&["topic", "info", &topic, "-v"], 8).await?;
    let topic_type = run_ros2(&["topic", "type", &topic], 6).await.ok();

    let info_raw = info.stdout.trim().to_string();
    let publishers = parse_count_field(&info_raw, "Publisher count");
    let subscribers = parse_count_field(&info_raw, "Subscription count");
    let type_text = topic_type
        .map(|v| v.stdout.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| parse_type_field(&info_raw));

    Ok(json!({
        "topic": topic,
        "type": type_text,
        "publisher_count": publishers,
        "subscription_count": subscribers,
        "raw": info_raw,
        "bridge_url": skill.bridge_url,
    }))
}

async fn subscribe_samples(skill: &Ros2Skill, params: &Value) -> ExecutionResult {
    let topic = required_topic(params)?;
    let requested = read_u64_param(params, "samples").unwrap_or(1).clamp(1, 10) as usize;
    let timeout_sec = read_u64_param(params, "timeout_sec")
        .unwrap_or(6)
        .clamp(2, 30);

    let mut messages = Vec::new();
    let mut errors = Vec::new();
    for _ in 0..requested {
        match run_ros2(&["topic", "echo", &topic, "--once"], timeout_sec).await {
            Ok(out) => {
                let msg = out.stdout.trim().to_string();
                if !msg.is_empty() {
                    messages.push(msg);
                }
            }
            Err(err) => errors.push(err.to_string()),
        }
    }

    if messages.is_empty() {
        return Err(SkillError::ExecutionFailed(format!(
            "no samples collected from topic {} (errors: {})",
            topic,
            errors.join("; ")
        )));
    }

    Ok(json!({
        "topic": topic,
        "requested_samples": requested,
        "samples_collected": messages.len(),
        "messages": messages,
        "errors": errors,
        "bridge_url": skill.bridge_url,
        "stream_hint": {
            "foxglove": "ws://localhost:8765",
            "rosbridge": "ws://localhost:9090",
        }
    }))
}

async fn topic_stats(skill: &Ros2Skill, params: &Value) -> ExecutionResult {
    let topic = required_topic(params)?;
    let window_sec = read_u64_param(params, "window_sec")
        .unwrap_or(6)
        .clamp(3, 60);

    let hz_cmd = format!(
        "timeout {}s ros2 topic hz '{}' 2>&1 || true",
        window_sec,
        shell_quote(&topic)
    );
    let bw_cmd = format!(
        "timeout {}s ros2 topic bw '{}' 2>&1 || true",
        window_sec,
        shell_quote(&topic)
    );
    let delay_cmd = format!(
        "timeout {}s ros2 topic delay '{}' 2>&1 || true",
        window_sec,
        shell_quote(&topic)
    );

    let hz_out = run_shell(&hz_cmd, window_sec + 3).await?;
    let bw_out = run_shell(&bw_cmd, window_sec + 3).await?;
    let delay_out = run_shell(&delay_cmd, window_sec + 3).await?;

    Ok(json!({
        "topic": topic,
        "window_sec": window_sec,
        "average_hz": parse_metric_value(&hz_out.stdout, "average rate:"),
        "average_bw": parse_bw_value(&bw_out.stdout),
        "average_delay_sec": parse_metric_value(&delay_out.stdout, "average delay:"),
        "raw": {
            "hz": hz_out.stdout.trim(),
            "bw": bw_out.stdout.trim(),
            "delay": delay_out.stdout.trim(),
        },
        "bridge_url": skill.bridge_url,
    }))
}

async fn stream_endpoint(skill: &Ros2Skill, params: &Value) -> ExecutionResult {
    let requested_transport = params
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("foxglove")
        .to_lowercase();
    let transport = match requested_transport.as_str() {
        "rosbridge" => "rosbridge".to_string(),
        _ => "foxglove".to_string(),
    };
    let default_port = if transport == "rosbridge" { 9090 } else { 8765 };
    let port = read_u64_param(params, "port")
        .unwrap_or(default_port as u64)
        .clamp(1, 65535) as u16;

    let socket = format!("127.0.0.1:{}", port);
    let bridge_url = format!("ws://localhost:{}", port);
    let status = match timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&socket),
    )
    .await
    {
        Ok(Ok(_)) => "reachable",
        _ => "unreachable",
    };

    Ok(json!({
        "transport": transport,
        "port": port,
        "status": status,
        "agent_local_endpoint": format!("ws://{}", socket),
        "cli_forward_endpoint": format!("ws://localhost:{}", port),
        "required_process": if transport == "rosbridge" { "rosbridge_server" } else { "foxglove_bridge" },
        "notes": [
            "Use `robotunnel connect <robot>` to establish the data plane.",
            "CLI will auto-forward 9090 and 8765 by default.",
            "For custom ports (including rviz web/VNC), use `robotunnel connect --forward <local>:<remote>`.",
            "Status reflects whether agent can TCP-connect to 127.0.0.1:<port>; unreachable usually means bridge process is not listening on robot side."
        ],
        "bridge_url": bridge_url,
        "default_rosbridge_url": skill.bridge_url,
    }))
}

async fn run_ros2(args: &[&str], timeout_sec: u64) -> Result<CmdOutput, SkillError> {
    let mut cmd = Command::new("ros2");
    cmd.args(args);
    run_command(cmd, timeout_sec).await
}

async fn run_shell(script: &str, timeout_sec: u64) -> Result<CmdOutput, SkillError> {
    let mut cmd = Command::new("sh");
    cmd.args(["-lc", script]);
    run_command(cmd, timeout_sec).await
}

async fn run_command(mut cmd: Command, timeout_sec: u64) -> Result<CmdOutput, SkillError> {
    let duration = Duration::from_secs(timeout_sec.clamp(2, 120));
    let output = timeout(duration, cmd.output())
        .await
        .map_err(|_| SkillError::Timeout(duration.as_secs()))?
        .map_err(|err| SkillError::ExecutionFailed(err.to_string()))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        let message = stderr.trim().chars().take(320).collect::<String>();
        return Err(SkillError::ExecutionFailed(if message.is_empty() {
            format!("command exited with status {}", output.status)
        } else {
            message
        }));
    }
    let _ = stderr;
    Ok(CmdOutput { stdout })
}

fn required_topic(params: &Value) -> Result<String, SkillError> {
    let topic = params
        .get("topic")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if topic.is_empty() {
        return Err(SkillError::InvalidParams(
            "missing required string param: topic".to_string(),
        ));
    }
    Ok(topic.to_string())
}

fn read_u64_param(params: &Value, key: &str) -> Option<u64> {
    match params.get(key) {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_topics_with_types(raw: &str) -> Vec<Value> {
    raw.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            if let Some(start) = line.rfind('[') {
                if line.ends_with(']') && start > 0 {
                    let name = line[..start].trim();
                    let topic_type = line[start + 1..line.len() - 1].trim();
                    if !name.is_empty() {
                        return Some(json!({"name": name, "type": topic_type}));
                    }
                }
            }
            Some(json!({"name": line, "type": serde_json::Value::Null}))
        })
        .collect()
}

fn parse_count_field(raw: &str, key: &str) -> Option<u64> {
    raw.lines().find_map(|line| {
        let line = line.trim();
        if !line.starts_with(key) {
            return None;
        }
        line.split(':')
            .nth(1)
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

fn parse_type_field(raw: &str) -> Option<String> {
    raw.lines().find_map(|line| {
        let line = line.trim();
        if !line.to_lowercase().starts_with("type:") {
            return None;
        }
        let v = line.split(':').nth(1)?.trim().to_string();
        if v.is_empty() {
            None
        } else {
            Some(v)
        }
    })
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

struct CmdOutput {
    stdout: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_topic_list_with_types() {
        let raw = "/scan [sensor_msgs/msg/LaserScan]\n/chatter [std_msgs/msg/String]\n";
        let parsed = parse_topics_with_types(raw);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], "/scan");
        assert_eq!(parsed[0]["type"], "sensor_msgs/msg/LaserScan");
    }

    #[test]
    fn parses_metric_value_from_hz_output() {
        let raw = "average rate: 9.987\n\tmin: 0.100s max: 0.101s std dev: 0.00013s window: 15";
        let metric = parse_metric_value(raw, "average rate:");
        assert_eq!(metric, Some(9.987));
    }
}
