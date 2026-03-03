//! rt-skill-fleet — Fleet state comparison skill.
//!
//! Collects telemetry snapshots from all connected robots (via the platform),
//! runs an LLM-powered diff to identify outliers, and returns a structured
//! natural-language report.
//!
//! Usage: invoked by the CLI via `robotunnel fleet compare --query "..."`

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use rt_llm::{InferRequest, LlmManager, Provider};

/// Telemetry snapshot from a single robot (sent by its rt-skill-monitor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobotTelemetry {
    pub robot_id: String,
    pub cpu_percent: f64,
    pub mem_percent: f64,
    pub disk_percent: f64,
    pub ros_node_count: Option<u32>,
    pub last_error: Option<String>,
    pub uptime_hours: f64,
}

/// Result of a fleet comparison query.
#[derive(Debug, Serialize)]
pub struct FleetCompareReport {
    pub query: String,
    pub robot_count: usize,
    pub outliers: Vec<String>,
    pub analysis: String,
}

/// Compare a fleet of robots against a natural-language query.
/// Returns a structured report with outlier identification and explanation.
pub async fn compare(
    query: &str,
    fleet: Vec<RobotTelemetry>,
    provider: &Provider,
) -> Result<FleetCompareReport> {
    info!("Fleet compare: {} robots, query: '{}'", fleet.len(), query);

    if fleet.is_empty() {
        return Ok(FleetCompareReport {
            query: query.to_string(),
            robot_count: 0,
            outliers: vec![],
            analysis: "No robots connected.".to_string(),
        });
    }

    // Build fleet summary for LLM context
    let fleet_json = serde_json::to_string_pretty(&fleet)?;
    let avg_cpu = fleet.iter().map(|r| r.cpu_percent).sum::<f64>() / fleet.len() as f64;
    let avg_mem = fleet.iter().map(|r| r.mem_percent).sum::<f64>() / fleet.len() as f64;

    let prompt = format!(
        "You are diagnosing a robot fleet. The user's question: \"{query}\"\n\n\
         Fleet average: CPU={avg_cpu:.1}%, RAM={avg_mem:.1}%\n\
         Robot telemetry:\n{fleet_json}\n\n\
         Identify any robots that are outliers compared to the fleet. \
         For each outlier, explain what metric differs, by how much, and suggest a likely cause. \
         Be concise and technical. Format: bullet points per robot.",
        query = query,
        avg_cpu = avg_cpu,
        avg_mem = avg_mem,
        fleet_json = fleet_json,
    );

    let mgr = LlmManager::open()?;
    let analysis = mgr.infer(
        provider,
        InferRequest::with_system(
            "You are a robotics fleet diagnostics assistant. Be precise, technical, and concise.",
            prompt,
        ),
    ).await?;

    // Identify outlier robot IDs (simple heuristic: >2x fleet avg CPU or RAM)
    let outliers: Vec<String> = fleet.iter()
        .filter(|r| r.cpu_percent > avg_cpu * 2.0 || r.mem_percent > avg_mem * 1.5)
        .map(|r| r.robot_id.clone())
        .collect();

    Ok(FleetCompareReport {
        query: query.to_string(),
        robot_count: fleet.len(),
        outliers,
        analysis,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_robot(id: &str, cpu: f64, mem: f64) -> RobotTelemetry {
        RobotTelemetry {
            robot_id: id.to_string(),
            cpu_percent: cpu,
            mem_percent: mem,
            disk_percent: 40.0,
            ros_node_count: Some(5),
            last_error: None,
            uptime_hours: 24.0,
        }
    }

    #[test]
    fn test_outlier_detection() {
        let fleet = vec![
            make_robot("robot-1", 20.0, 40.0),
            make_robot("robot-2", 22.0, 42.0),
            make_robot("robot-3", 85.0, 80.0), // outlier
            make_robot("robot-4", 19.0, 38.0),
        ];
        let avg_cpu = fleet.iter().map(|r| r.cpu_percent).sum::<f64>() / fleet.len() as f64;
        let avg_mem = fleet.iter().map(|r| r.mem_percent).sum::<f64>() / fleet.len() as f64;
        let outliers: Vec<String> = fleet.iter()
            .filter(|r| r.cpu_percent > avg_cpu * 2.0 || r.mem_percent > avg_mem * 1.5)
            .map(|r| r.robot_id.clone())
            .collect();
        assert!(outliers.contains(&"robot-3".to_string()));
        assert!(!outliers.contains(&"robot-1".to_string()));
    }
}
