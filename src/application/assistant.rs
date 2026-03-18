use super::contracts::BuiltinContracts;
use super::projection_plane::ProjectionEngine;
use rt_agent_dispatch::Skill;
use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use rt_llm::{InferRequest, LlmManager, Provider};
use rt_skill_ros2_observe::Ros2Skill;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolCallSpec {
    skill: String,
    action: String,
    #[serde(default = "empty_object")]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct ModelDecision {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    reply: String,
    #[serde(default)]
    summary: String,
    #[serde(default)]
    needs_confirmation: bool,
    #[serde(default)]
    confirmation_reason: String,
    tool: Option<ToolCallSpec>,
}

#[derive(Debug, Clone)]
struct AssistantDecision {
    reply: String,
    summary: String,
    needs_confirmation: bool,
    confirmation_reason: String,
    tool: Option<ToolCallSpec>,
}

pub(super) async fn handle_assistant_skill(
    req: CommandRequest,
    tx: broadcast::Sender<Vec<u8>>,
    contracts: Arc<BuiltinContracts>,
    ros2_observe_tool: Arc<Ros2Skill>,
    projection_engine: Arc<ProjectionEngine>,
) -> CommandResponse {
    if req.action != "route" {
        return error_response(req.id, format!("unknown assistant action: {}", req.action));
    }

    let query = req
        .params
        .get("query")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_string())
        .unwrap_or_default();
    if query.is_empty() {
        return error_response(req.id, "missing required param 'query'".to_string());
    }

    let allow_risky = req
        .params
        .get("allow_risky")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let context = req
        .params
        .get("context")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let provider = resolve_provider(&req.params);
    let provider_name = provider_name(&provider);
    let forced_call = req.params.get("force_call").and_then(parse_tool_call_value);

    let decision = match forced_call {
        Some(call) => AssistantDecision {
            reply: String::new(),
            summary: format!("Run {}.{} from approved plan", call.skill, call.action),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: Some(call),
        },
        None => plan_with_model_or_heuristics(&query, &context, &provider).await,
    };

    if let Some(tool) = decision.tool.clone() {
        let risky = is_risky_call(&tool);
        if (decision.needs_confirmation || risky) && !allow_risky {
            let summary = if decision.summary.trim().is_empty() {
                format!("Run {}.{}", tool.skill, tool.action)
            } else {
                decision.summary.clone()
            };
            let reason = if decision.confirmation_reason.trim().is_empty() {
                "This action can mutate robot state or execute shell commands.".to_string()
            } else {
                decision.confirmation_reason.clone()
            };
            let reply = if decision.reply.trim().is_empty() {
                format!(
                    "I can run `{}` on this robot, but it requires your confirmation.",
                    summary
                )
            } else {
                decision.reply.clone()
            };
            return ok_response(
                req.id,
                json!({
                    "reply": reply,
                    "summary": summary,
                    "needs_confirmation": true,
                    "confirmation_reason": reason,
                    "proposed_call": tool,
                    "provider": provider_name,
                }),
            );
        }

        let tool_resp = execute_local_tool_call(
            &req.id,
            tool.clone(),
            tx,
            contracts.as_ref(),
            ros2_observe_tool,
            projection_engine,
        )
        .await;
        let reply = if decision.reply.trim().is_empty() {
            format_tool_reply(&tool, &tool_resp)
        } else {
            decision.reply
        };
        let tool_response_value = serde_json::to_value(&tool_resp).unwrap_or_else(|_| {
            json!({
                "id": tool_resp.id,
                "status": "error",
                "error": "failed to serialize tool response",
            })
        });
        return ok_response(
            req.id,
            json!({
                "reply": reply,
                "summary": decision.summary,
                "needs_confirmation": false,
                "executed_call": tool,
                "tool_response": tool_response_value,
                "provider": provider_name,
            }),
        );
    }

    let reply = if decision.reply.trim().is_empty() {
        "I need more details. Tell me which robot signal you want to inspect (status, logs, ros2 topics, visual projection, monitor)."
            .to_string()
    } else {
        decision.reply
    };
    ok_response(
        req.id,
        json!({
            "reply": reply,
            "summary": decision.summary,
            "needs_confirmation": false,
            "provider": provider_name,
        }),
    )
}

fn resolve_provider(params: &Value) -> Provider {
    let configured = params
        .get("provider")
        .and_then(Value::as_str)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            std::env::var("RT_AGENT_INTENT_PROVIDER")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .unwrap_or_else(|| "kimi".to_string());
    Provider::from_str(&configured).unwrap_or(Provider::Kimi)
}

fn provider_name(provider: &Provider) -> &'static str {
    match provider {
        Provider::OpenAI => "openai",
        Provider::Claude => "claude",
        Provider::Gemini => "gemini",
        Provider::Grok => "grok",
        Provider::DeepSeek => "deepseek",
        Provider::MiniMax => "minimax",
        Provider::Kimi => "kimi",
        Provider::Qwen => "qwen",
    }
}

async fn plan_with_model_or_heuristics(
    query: &str,
    context: &Value,
    provider: &Provider,
) -> AssistantDecision {
    if let Some(model_decision) = plan_with_model(query, context, provider).await {
        return model_decision;
    }
    heuristic_plan(query)
}

async fn plan_with_model(
    query: &str,
    context: &Value,
    provider: &Provider,
) -> Option<AssistantDecision> {
    let manager = LlmManager::open().ok()?;
    let context_json = serde_json::to_string(context).ok()?;
    let user_prompt = format!(
        "context={}\nquery={}\nReturn JSON only.",
        context_json, query
    );
    let raw = manager
        .infer(
            provider,
            InferRequest {
                system: Some(assistant_system_prompt().to_string()),
                user: user_prompt,
                max_tokens: 700,
            },
        )
        .await
        .ok()?;
    parse_model_decision(&raw)
}

fn assistant_system_prompt() -> &'static str {
    r#"You are the on-robot intent planner for RoboTunnel.
You run on the robot and can call local tools.

Available tools:
- host_debug.status {}
- host_debug.logs {"unit"?:string,"lines"?:integer,"since"?:string}
- host_debug.shell {"cmd":string,"timeout"?:integer} (risky)
- monitor.status {}
- monitor.snapshot {}
- ros2_observe.list_topics {}
- ros2_observe.topic_info {"topic":string}
- ros2_observe.subscribe {"topic":string,"samples"?:integer,"timeout_sec"?:integer}
- ros2_observe.topic_stats {"topic":string,"window_sec"?:integer}
- ros2_observe.stream_endpoint {"transport"?:string,"port"?:integer}
- visual_debug.list_profiles {}
- visual_debug.start {"mode"?:string,"profile"?:string,"transport_policy"?:string,"topics"?:array,"desired_delay_ms"?:integer,"tf_alignment_window_ms"?:integer,"topic_policy"?:object}
- visual_debug.stop {"session_id":string}
- visual_debug.status {"session_id"?:string}
- visual_debug.recommend {"mode"?:string,"transport_policy"?:string,"topics"?:array,"session_id"?:string}
- visual_debug.topic_stats {"topic":string,"session_id"?:string,"window_sec"?:integer}
- visual_debug.stream_pull {"session_id":string,"topic":string,"since_seq"?:integer,"limit"?:integer}
- system.config_get {"section":"monitor|visual_debug"}
- system.config_set {"section":"monitor|visual_debug","settings":{...}} (risky)

Rules:
1) Output JSON only.
2) Choose at most one tool call.
3) Prefer monitor/host_debug/ros2_observe/visual_debug for robot diagnostics.
4) For risky calls (host_debug.shell, system.config_set), set needs_confirmation=true.
5) If no tool is needed, use kind='respond' with a concise reply.

Response schema:
{
  "kind":"call_tool|respond",
  "reply":"string",
  "summary":"string",
  "needs_confirmation":false,
  "confirmation_reason":"",
  "tool":{"skill":"...","action":"...","params":{}}
}
"#
}

fn parse_model_decision(raw: &str) -> Option<AssistantDecision> {
    let blob = extract_json_object(raw)?;
    let parsed: ModelDecision = serde_json::from_str(&blob).ok()?;
    let kind = parsed.kind.trim().to_lowercase();
    let tool = parsed.tool.and_then(normalize_tool_call);
    match kind.as_str() {
        "respond" => Some(AssistantDecision {
            reply: parsed.reply.trim().to_string(),
            summary: parsed.summary.trim().to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: None,
        }),
        "call_tool" => {
            let tool = tool?;
            Some(AssistantDecision {
                reply: parsed.reply.trim().to_string(),
                summary: parsed.summary.trim().to_string(),
                needs_confirmation: parsed.needs_confirmation,
                confirmation_reason: parsed.confirmation_reason.trim().to_string(),
                tool: Some(tool),
            })
        }
        _ => None,
    }
}

fn heuristic_plan(query: &str) -> AssistantDecision {
    let lower = query.to_lowercase();
    if lower.contains("log")
        || lower.contains("日志")
        || lower.contains("报错")
        || lower.contains("error")
    {
        return AssistantDecision {
            reply: String::new(),
            summary: "Inspect recent robot logs".to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: Some(ToolCallSpec {
                skill: "host_debug".to_string(),
                action: "logs".to_string(),
                params: json!({ "lines": 80 }),
            }),
        };
    }
    if lower.contains("shell") || lower.contains("命令") || lower.contains("执行") {
        if let Some(cmd) = extract_backtick_block(query) {
            return AssistantDecision {
                reply: "This requires approval before I run a shell command.".to_string(),
                summary: "Run host_debug.shell".to_string(),
                needs_confirmation: true,
                confirmation_reason: "Shell execution can modify robot state.".to_string(),
                tool: Some(ToolCallSpec {
                    skill: "host_debug".to_string(),
                    action: "shell".to_string(),
                    params: json!({ "cmd": cmd }),
                }),
            };
        }
        return AssistantDecision {
            reply:
                "To run a shell command, include it in backticks, for example: `ros2 node list`."
                    .to_string(),
            summary: "Need explicit shell command".to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: None,
        };
    }
    if lower.contains("rviz")
        || lower.contains("foxglove")
        || lower.contains("projection")
        || lower.contains("visual debug")
        || lower.contains("可视化")
    {
        return AssistantDecision {
            reply: String::new(),
            summary: "Start visual debug projection session".to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: Some(ToolCallSpec {
                skill: "visual_debug".to_string(),
                action: "start".to_string(),
                params: json!({"mode":"foxglove","transport_policy":"tcp_only","profile":"balanced"}),
            }),
        };
    }
    if lower.contains("ros") || lower.contains("topic") || lower.contains("话题") {
        if let Some(topic) = extract_topic_name(query) {
            return AssistantDecision {
                reply: String::new(),
                summary: "Inspect ROS 2 topic details".to_string(),
                needs_confirmation: false,
                confirmation_reason: String::new(),
                tool: Some(ToolCallSpec {
                    skill: "ros2_observe".to_string(),
                    action: "topic_info".to_string(),
                    params: json!({ "topic": topic }),
                }),
            };
        }
        return AssistantDecision {
            reply: String::new(),
            summary: "List ROS 2 topics".to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: Some(ToolCallSpec {
                skill: "ros2_observe".to_string(),
                action: "list_topics".to_string(),
                params: json!({}),
            }),
        };
    }
    if lower.contains("cpu")
        || lower.contains("内存")
        || lower.contains("磁盘")
        || lower.contains("状态")
        || lower.contains("monitor")
        || lower.contains("health")
    {
        return AssistantDecision {
            reply: String::new(),
            summary: "Collect monitor snapshot".to_string(),
            needs_confirmation: false,
            confirmation_reason: String::new(),
            tool: Some(ToolCallSpec {
                skill: "monitor".to_string(),
                action: "status".to_string(),
                params: json!({}),
            }),
        };
    }

    AssistantDecision {
        reply: String::new(),
        summary: "Collect robot host status".to_string(),
        needs_confirmation: false,
        confirmation_reason: String::new(),
        tool: Some(ToolCallSpec {
            skill: "host_debug".to_string(),
            action: "status".to_string(),
            params: json!({}),
        }),
    }
}

async fn execute_local_tool_call(
    parent_request_id: &str,
    tool: ToolCallSpec,
    tx: broadcast::Sender<Vec<u8>>,
    contracts: &BuiltinContracts,
    ros2_observe_tool: Arc<Ros2Skill>,
    projection_engine: Arc<ProjectionEngine>,
) -> CommandResponse {
    if tool.skill == "assistant" {
        return error_response(
            format!("{}-assistant-tool", parent_request_id),
            "assistant cannot call itself".to_string(),
        );
    }

    let sub_request = CommandRequest {
        id: format!("{}-assistant-tool", parent_request_id),
        skill: tool.skill.clone(),
        action: tool.action.clone(),
        params: tool.params.clone(),
    };
    if let Err(err) = contracts.validate(&sub_request) {
        return error_response(sub_request.id, err);
    }

    match tool.skill.as_str() {
        "host_debug" => rt_skill_host_debug::handle(sub_request, tx).await,
        "monitor" => super::handle_monitor_skill(sub_request).await,
        "fleet" => super::handle_fleet_skill(sub_request).await,
        "acceptance" => super::handle_acceptance_skill(sub_request).await,
        "system" => super::handle_system_skill(sub_request, contracts).await,
        "ros2_observe" => {
            let action = sub_request.action.clone();
            let params = sub_request.params.clone();
            let id = sub_request.id.clone();
            match ros2_observe_tool.execute(&action, params, tx).await {
                Ok(data) => CommandResponse {
                    id,
                    status: CommandStatus::Ok,
                    data: Some(data),
                    error: None,
                },
                Err(err) => CommandResponse {
                    id,
                    status: CommandStatus::Error,
                    data: None,
                    error: Some(err.to_string()),
                },
            }
        }
        "visual_debug" => {
            super::visual_debug::handle_visual_debug_skill(sub_request, projection_engine).await
        }
        _ => error_response(
            sub_request.id,
            format!("unsupported tool call: {}.{}", tool.skill, tool.action),
        ),
    }
}

fn is_risky_call(tool: &ToolCallSpec) -> bool {
    (tool.skill == "host_debug" && tool.action == "shell")
        || (tool.skill == "system" && tool.action == "config_set")
}

fn normalize_tool_call(mut call: ToolCallSpec) -> Option<ToolCallSpec> {
    call.skill = call.skill.trim().to_string();
    call.action = call.action.trim().to_string();
    if call.skill.is_empty() || call.action.is_empty() {
        return None;
    }
    if !call.params.is_object() {
        call.params = json!({});
    }
    Some(call)
}

fn parse_tool_call_value(value: &Value) -> Option<ToolCallSpec> {
    serde_json::from_value::<ToolCallSpec>(value.clone())
        .ok()
        .and_then(normalize_tool_call)
}

fn format_tool_reply(tool: &ToolCallSpec, response: &CommandResponse) -> String {
    if response.status != CommandStatus::Ok {
        return format!(
            "Tool `{}` failed: {}",
            format!("{}.{}", tool.skill, tool.action),
            response
                .error
                .as_deref()
                .unwrap_or("unknown execution error")
        );
    }

    let Some(data) = response.data.as_ref() else {
        return format!(
            "Tool `{}` completed with no data.",
            format!("{}.{}", tool.skill, tool.action)
        );
    };

    match (tool.skill.as_str(), tool.action.as_str()) {
        ("monitor", "status") | ("monitor", "snapshot") => {
            let cpu = number_field(data, "cpu_percent").unwrap_or(0.0);
            let mem = number_field(data, "mem_percent").unwrap_or(0.0);
            let disk = number_field(data, "disk_used_gb").unwrap_or(0.0);
            format!(
                "Robot health snapshot: CPU {:.1}%, memory {:.1}%, disk used {:.1} GB.",
                cpu, mem, disk
            )
        }
        ("host_debug", "status") => {
            let hostname = string_field(data, "hostname").unwrap_or("unknown");
            let uptime = string_field(data, "uptime").unwrap_or("unknown");
            let mem = nested_number_field(data, &["memory", "used_percent"]).unwrap_or(0.0);
            let disk = nested_number_field(data, &["disk", "used_percent"]).unwrap_or(0.0);
            format!(
                "Robot `{}` uptime {}. Memory {:.1}% used, disk {:.1}% used.",
                hostname, uptime, mem, disk
            )
        }
        ("host_debug", "logs") => {
            let logs = string_field(data, "logs").unwrap_or("");
            let tail = tail_lines(logs, 12, 1000);
            if tail.is_empty() {
                "No recent logs returned.".to_string()
            } else {
                format!("Recent logs:\n{}", tail)
            }
        }
        ("ros2_observe", "list_topics") => {
            let topics = data
                .get("topics")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            v.get("name")
                                .and_then(|name| name.as_str())
                                .map(ToString::to_string)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if topics.is_empty() {
                "ROS 2 topics: none reported.".to_string()
            } else {
                format!("ROS 2 topics ({}): {}", topics.len(), topics.join(", "))
            }
        }
        ("ros2_observe", "topic_info") => {
            let topic = string_field(data, "topic").unwrap_or("unknown");
            let topic_type = string_field(data, "type").unwrap_or("unknown");
            format!("ROS 2 topic `{}` type: `{}`.", topic, topic_type)
        }
        ("ros2_observe", "subscribe") => {
            let topic = string_field(data, "topic").unwrap_or("unknown");
            let collected = data
                .get("samples_collected")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            format!(
                "Collected {} real sample(s) from ROS 2 topic `{}`.",
                collected, topic
            )
        }
        ("ros2_observe", "topic_stats") => {
            let topic = string_field(data, "topic").unwrap_or("unknown");
            let hz = number_field(data, "average_hz").unwrap_or(0.0);
            let delay = number_field(data, "average_delay_sec").unwrap_or(0.0);
            let bw = string_field(data, "average_bw").unwrap_or("unknown");
            format!(
                "Topic `{}` stats: {:.2} Hz, delay {:.4}s, bandwidth {}.",
                topic, hz, delay, bw
            )
        }
        ("ros2_observe", "stream_endpoint") => {
            let endpoint =
                string_field(data, "cli_forward_endpoint").unwrap_or("ws://localhost:8765");
            let transport = string_field(data, "transport").unwrap_or("foxglove");
            let status = string_field(data, "status").unwrap_or("unknown");
            format!(
                "ROS stream endpoint ({}) is {} at {}.",
                transport, status, endpoint
            )
        }
        ("visual_debug", "start") => {
            let session_id =
                nested_string_field(data, &["session", "session_id"]).unwrap_or("unknown");
            let mode = nested_string_field(data, &["session", "mode"]).unwrap_or("unknown");
            let profile = nested_string_field(data, &["session", "profile"]).unwrap_or("unknown");
            format!(
                "Visual debug projection started: session `{}`, mode `{}`, profile `{}`.",
                session_id, mode, profile
            )
        }
        ("visual_debug", "status") => {
            let count = data.get("count").and_then(Value::as_u64).unwrap_or(0);
            format!("Visual debug has {} active projection session(s).", count)
        }
        ("visual_debug", "recommend") => {
            let profile =
                nested_string_field(data, &["recommendation", "profile"]).unwrap_or("balanced");
            let reasons = nested_array_len(data, &["recommendation", "reasons"]);
            format!(
                "Visual debug recommendation: profile `{}` ({} reason item(s)).",
                profile, reasons
            )
        }
        ("visual_debug", "topic_stats") => {
            let topic = nested_string_field(data, &["stats", "topic"]).unwrap_or("unknown");
            let source = string_field(data, "source").unwrap_or("unknown");
            let collector_sparse = data
                .get("collector_sparse")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let runtime_status =
                nested_string_field(data, &["runtime_projection", "stats", "status"])
                    .unwrap_or("unknown");
            let last_success_age =
                nested_u64_field(data, &["runtime_projection", "stats", "last_success_age"])
                    .unwrap_or(0);
            format!(
                "Visual debug topic stats for `{}`: source={}, status={}, stale_age={}s, collector_sparse={}.",
                topic, source, runtime_status, last_success_age, collector_sparse
            )
        }
        ("visual_debug", "stream_pull") => {
            let topic = nested_string_field(data, &["stream", "topic"]).unwrap_or("unknown");
            let returned = nested_u64_field(data, &["stream", "returned"]).unwrap_or(0);
            let last_seq = nested_u64_field(data, &["stream", "last_seq"]).unwrap_or(0);
            format!(
                "Pulled {} projected message(s) for `{}` (last_seq={}).",
                returned, topic, last_seq
            )
        }
        _ => serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".to_string()),
    }
}

fn extract_json_object(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut start: Option<usize> = None;
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, b) in bytes.iter().enumerate() {
        match *b {
            b'\\' if in_string => {
                escaped = !escaped;
                continue;
            }
            b'"' if !escaped => {
                in_string = !in_string;
            }
            b'{' if !in_string => {
                if start.is_none() {
                    start = Some(idx);
                }
                depth += 1;
            }
            b'}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    if let Some(s) = start {
                        return Some(raw[s..=idx].to_string());
                    }
                }
            }
            _ => {}
        }
        if *b != b'\\' {
            escaped = false;
        }
    }
    None
}

fn extract_backtick_block(query: &str) -> Option<String> {
    let start = query.find('`')?;
    let rest = &query[start + 1..];
    let end = rest.find('`')?;
    let cmd = rest[..end].trim();
    if cmd.is_empty() {
        None
    } else {
        Some(cmd.to_string())
    }
}

fn extract_topic_name(query: &str) -> Option<String> {
    for token in query.split_whitespace() {
        let cleaned = token.trim_matches(|c: char| ",.;:()[]{}".contains(c));
        if cleaned.starts_with('/') && cleaned.len() > 1 {
            return Some(cleaned.to_string());
        }
    }
    None
}

fn tail_lines(text: &str, max_lines: usize, max_chars: usize) -> String {
    let mut lines = text
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() > max_lines {
        lines = lines[lines.len() - max_lines..].to_vec();
    }
    let mut out = lines.join("\n");
    if out.len() > max_chars {
        out = out[out.len().saturating_sub(max_chars)..].to_string();
    }
    out
}

fn string_field<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key).and_then(Value::as_str)
}

fn number_field(data: &Value, key: &str) -> Option<f64> {
    data.get(key).and_then(Value::as_f64)
}

fn nested_string_field<'a>(data: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut current = data;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn nested_number_field(data: &Value, path: &[&str]) -> Option<f64> {
    let mut current = data;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_f64()
}

fn nested_u64_field(data: &Value, path: &[&str]) -> Option<u64> {
    let mut current = data;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_u64()
}

fn nested_array_len(data: &Value, path: &[&str]) -> usize {
    let mut current = data;
    for key in path {
        let Some(next) = current.get(*key) else {
            return 0;
        };
        current = next;
    }
    current.as_array().map(|items| items.len()).unwrap_or(0)
}

fn ok_response(id: String, data: Value) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Ok,
        data: Some(data),
        error: None,
    }
}

fn error_response(id: String, msg: String) -> CommandResponse {
    CommandResponse {
        id,
        status: CommandStatus::Error,
        data: None,
        error: Some(msg),
    }
}

fn empty_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_object() {
        let raw = "text before {\"kind\":\"respond\",\"reply\":\"ok\"} after";
        let blob = extract_json_object(raw).expect("json");
        assert!(blob.contains("\"kind\":\"respond\""));
    }

    #[test]
    fn test_heuristic_shell_requires_confirmation() {
        let decision = heuristic_plan("请执行 `ros2 node list`");
        assert!(decision.needs_confirmation);
        let tool = decision.tool.expect("tool");
        assert_eq!(tool.skill, "host_debug");
        assert_eq!(tool.action, "shell");
    }
}
