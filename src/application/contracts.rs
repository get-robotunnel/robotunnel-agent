use rt_core::protocol::CommandRequest;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const BUILTIN_CONTRACT_VERSION: &str = "builtin-skill-contract/v1";

pub struct BuiltinContracts {
    skills: BTreeMap<&'static str, SkillContract>,
}

#[derive(Clone, Serialize)]
pub struct SkillContract {
    pub name: &'static str,
    pub kind: &'static str,
    pub description: &'static str,
    pub actions: Vec<ActionContract>,
}

#[derive(Clone, Serialize)]
pub struct ActionContract {
    pub name: &'static str,
    pub description: &'static str,
    pub params: Vec<ParamContract>,
}

#[derive(Clone, Serialize)]
pub struct ParamContract {
    pub name: &'static str,
    pub description: &'static str,
    pub required: bool,
    pub kind: ParamType,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum ParamType {
    String,
    Integer,
    Number,
    Boolean,
    Object,
    Array,
}

impl BuiltinContracts {
    pub fn new() -> Self {
        let mut skills = BTreeMap::new();
        skills.insert(
            "system",
            SkillContract {
                name: "system",
                kind: "builtin",
                description: "Agent metadata and capability discovery.",
                actions: vec![
                    ActionContract {
                        name: "capabilities",
                        description: "Return machine-readable contracts for built-in skills.",
                        params: vec![],
                    },
                    ActionContract {
                        name: "config_get",
                        description: "Read a structured local agent config section.",
                        params: vec![param(
                            "section",
                            ParamType::String,
                            true,
                            "Config section name, for example 'monitor'.",
                        )],
                    },
                    ActionContract {
                        name: "config_set",
                        description: "Update a structured local agent config section.",
                        params: vec![
                            param(
                                "section",
                                ParamType::String,
                                true,
                                "Config section name, for example 'monitor'.",
                            ),
                            param(
                                "settings",
                                ParamType::Object,
                                true,
                                "Partial structured config payload.",
                            ),
                        ],
                    },
                ],
            },
        );
        skills.insert(
            "debug",
            SkillContract {
                name: "debug",
                kind: "builtin",
                description: "Robot debugging and operational inspection.",
                actions: vec![
                    ActionContract {
                        name: "status",
                        description: "Return host status, uptime, load, memory, and disk.",
                        params: vec![],
                    },
                    ActionContract {
                        name: "logs",
                        description: "Return journal logs from the robot.",
                        params: vec![
                            param(
                                "unit",
                                ParamType::String,
                                false,
                                "Optional systemd unit filter.",
                            ),
                            param(
                                "lines",
                                ParamType::Integer,
                                false,
                                "Number of log lines to return.",
                            ),
                            param("since", ParamType::String, false, "Journal time filter."),
                        ],
                    },
                    ActionContract {
                        name: "shell",
                        description: "Execute a shell command when debug shell access is enabled.",
                        params: vec![
                            param("cmd", ParamType::String, true, "Shell command to execute."),
                            param("timeout", ParamType::Integer, false, "Timeout in seconds."),
                        ],
                    },
                ],
            },
        );
        skills.insert(
            "ros2",
            SkillContract {
                name: "ros2",
                kind: "builtin",
                description: "ROS 2 bridge operations.",
                actions: vec![
                    ActionContract {
                        name: "list_topics",
                        description: "List ROS 2 topics exposed by the robot.",
                        params: vec![],
                    },
                    ActionContract {
                        name: "topic_info",
                        description: "Inspect a ROS 2 topic.",
                        params: vec![param("topic", ParamType::String, true, "ROS 2 topic name.")],
                    },
                    ActionContract {
                        name: "subscribe",
                        description: "Subscribe to a ROS 2 topic stream.",
                        params: vec![param("topic", ParamType::String, true, "ROS 2 topic name.")],
                    },
                ],
            },
        );
        skills.insert(
            "monitor",
            SkillContract {
                name: "monitor",
                kind: "builtin",
                description: "Robot health and telemetry snapshots.",
                actions: vec![
                    ActionContract {
                        name: "snapshot",
                        description: "Collect the current health snapshot.",
                        params: vec![],
                    },
                    ActionContract {
                        name: "status",
                        description: "Alias for monitor snapshot.",
                        params: vec![],
                    },
                ],
            },
        );
        skills.insert(
            "fleet",
            SkillContract {
                name: "fleet",
                kind: "builtin",
                description: "Fleet-wide comparison and outlier analysis.",
                actions: vec![ActionContract {
                    name: "compare",
                    description: "Compare robot state against a fleet payload.",
                    params: vec![
                        param("query", ParamType::String, false, "Comparison prompt."),
                        param(
                            "provider",
                            ParamType::String,
                            false,
                            "Local LLM provider name.",
                        ),
                        param("fleet", ParamType::Array, false, "Fleet telemetry payload."),
                    ],
                }],
            },
        );
        skills.insert(
            "acceptance",
            SkillContract {
                name: "acceptance",
                kind: "builtin",
                description: "Task validation and pass/fail reporting.",
                actions: vec![
                    ActionContract {
                        name: "run",
                        description: "Run an acceptance test from observations.",
                        params: vec![
                            param("task", ParamType::String, false, "Task prompt to evaluate."),
                            param(
                                "provider",
                                ParamType::String,
                                false,
                                "Local LLM provider name.",
                            ),
                            param(
                                "observations",
                                ParamType::Array,
                                false,
                                "Robot observation payload.",
                            ),
                        ],
                    },
                    ActionContract {
                        name: "test",
                        description: "Alias for acceptance run.",
                        params: vec![
                            param("task", ParamType::String, false, "Task prompt to evaluate."),
                            param(
                                "provider",
                                ParamType::String,
                                false,
                                "Local LLM provider name.",
                            ),
                            param(
                                "observations",
                                ParamType::Array,
                                false,
                                "Robot observation payload.",
                            ),
                        ],
                    },
                ],
            },
        );

        Self { skills }
    }

    pub fn capabilities_payload(&self) -> Value {
        let skills = self.skills.values().cloned().collect::<Vec<_>>();
        json!({
            "contract_version": BUILTIN_CONTRACT_VERSION,
            "skills": skills,
        })
    }

    pub fn validate(&self, req: &CommandRequest) -> Result<(), String> {
        let skill = self
            .skills
            .get(req.skill.as_str())
            .ok_or_else(|| format!("unknown skill: {}", req.skill))?;
        let action = skill
            .actions
            .iter()
            .find(|action| action.name == req.action)
            .ok_or_else(|| format!("unknown action '{}' for skill '{}'", req.action, req.skill))?;

        let params = match &req.params {
            Value::Null => None,
            Value::Object(map) => Some(map),
            _ if action.params.is_empty() => {
                return Err(format!(
                    "skill '{}.{}' does not accept params",
                    req.skill, req.action
                ));
            }
            _ => {
                return Err(format!(
                    "params for '{}.{}' must be a JSON object",
                    req.skill, req.action
                ));
            }
        };

        if let Some(map) = params {
            for key in map.keys() {
                if !action.params.iter().any(|param| param.name == key) {
                    return Err(format!(
                        "unknown param '{}' for '{}.{}'",
                        key, req.skill, req.action
                    ));
                }
            }
        }

        for param in &action.params {
            match params.and_then(|map| map.get(param.name)) {
                Some(value) => {
                    if !param.kind.matches(value) {
                        return Err(format!(
                            "param '{}' for '{}.{}' must be {}",
                            param.name,
                            req.skill,
                            req.action,
                            param.kind.as_str()
                        ));
                    }
                }
                None if param.required => {
                    return Err(format!(
                        "missing required param '{}' for '{}.{}'",
                        param.name, req.skill, req.action
                    ));
                }
                None => {}
            }
        }

        Ok(())
    }
}

impl ParamType {
    fn matches(&self, value: &Value) -> bool {
        match self {
            ParamType::String => value.is_string(),
            ParamType::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            ParamType::Number => value.is_number(),
            ParamType::Boolean => value.is_boolean(),
            ParamType::Object => value.is_object(),
            ParamType::Array => value.is_array(),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            ParamType::String => "string",
            ParamType::Integer => "integer",
            ParamType::Number => "number",
            ParamType::Boolean => "boolean",
            ParamType::Object => "object",
            ParamType::Array => "array",
        }
    }
}

fn param(
    name: &'static str,
    kind: ParamType,
    required: bool,
    description: &'static str,
) -> ParamContract {
    ParamContract {
        name,
        description,
        required,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_rejects_unknown_param() {
        let registry = BuiltinContracts::new();
        let req = CommandRequest {
            id: "1".to_string(),
            skill: "debug".to_string(),
            action: "status".to_string(),
            params: json!({"extra": true}),
        };
        let err = registry.validate(&req).unwrap_err();
        assert!(err.contains("unknown param"));
    }

    #[test]
    fn test_validate_requires_required_param() {
        let registry = BuiltinContracts::new();
        let req = CommandRequest {
            id: "1".to_string(),
            skill: "debug".to_string(),
            action: "shell".to_string(),
            params: json!({}),
        };
        let err = registry.validate(&req).unwrap_err();
        assert!(err.contains("missing required param 'cmd'"));
    }
}
