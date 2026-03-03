//! rt-skill-acceptance — Non-technical acceptance testing skill.
//!
//! Accepts a plain-language task description, uses the LLM to decompose it
//! into observable checks, runs each check against the fleet, and returns
//! a structured PASS/FAIL report.
//!
//! Usage: `robotunnel fleet test --task "Confirm all robots can complete a pick task"`

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use rt_llm::{InferRequest, LlmManager, Provider};

/// The result for a single robot in the acceptance test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobotTestResult {
    pub robot_id: String,
    pub passed: bool,
    pub details: String,
    /// Cycle time in seconds (if measurable)
    pub cycle_time_secs: Option<f64>,
}

/// Overall acceptance test report.
#[derive(Debug, Serialize)]
pub struct AcceptanceReport {
    pub task: String,
    pub total_robots: usize,
    pub passed: usize,
    pub failed: usize,
    pub results: Vec<RobotTestResult>,
    pub summary: String,
}

impl AcceptanceReport {
    pub fn pass_rate(&self) -> f64 {
        if self.total_robots == 0 { 0.0 }
        else { self.passed as f64 / self.total_robots as f64 * 100.0 }
    }
}

/// Run an acceptance test across a fleet.
///
/// `robot_results` contains raw per-robot observable data (already collected
/// by the runtime from each agent). The LLM evaluates pass/fail per robot
/// and generates a human-readable summary.
pub async fn run_acceptance_test(
    task: &str,
    robot_results: Vec<RobotObservation>,
    provider: &Provider,
) -> Result<AcceptanceReport> {
    info!("Acceptance test: '{}' across {} robots", task, robot_results.len());

    if robot_results.is_empty() {
        return Ok(AcceptanceReport {
            task: task.to_string(),
            total_robots: 0,
            passed: 0,
            failed: 0,
            results: vec![],
            summary: "No robots connected.".to_string(),
        });
    }

    let mgr = LlmManager::open()?;

    // Step 1: Decompose the task into observable checks
    let decompose_prompt = format!(
        "The user wants to verify: \"{task}\"\n\
         List 3-5 specific, observable checks that can be verified remotely from system telemetry \
         and ROS topic data. Format: numbered list, each check on one line.",
        task = task
    );
    let checks = mgr.infer(
        provider,
        InferRequest::with_system("You are a robotics QA engineer. Be concise.", decompose_prompt),
    ).await?;

    // Step 2: Evaluate each robot against the checks
    let observations_json = serde_json::to_string_pretty(&robot_results)?;
    let eval_prompt = format!(
        "Task to verify: \"{task}\"\n\
         Verification checks:\n{checks}\n\n\
         Robot observations:\n{observations}\n\n\
         For each robot, determine PASS or FAIL and explain why in one sentence. \
         Format: ROBOT_ID: PASS|FAIL — reason",
        task = task,
        checks = checks,
        observations = observations_json
    );

    let eval_result = mgr.infer(
        provider,
        InferRequest::with_system("You are a robotics QA engineer. Be concise and precise.", eval_prompt),
    ).await?;

    // Parse robot-level results from LLM response
    let results: Vec<RobotTestResult> = robot_results.iter().map(|obs| {
        let line = eval_result.lines()
            .find(|l| l.contains(&obs.robot_id))
            .unwrap_or("");
        let passed = line.contains("PASS");
        let details = line.split('—').nth(1)
            .unwrap_or("No evaluation available")
            .trim()
            .to_string();
        RobotTestResult {
            robot_id: obs.robot_id.clone(),
            passed,
            details,
            cycle_time_secs: obs.cycle_time_secs,
        }
    }).collect();

    let passed = results.iter().filter(|r| r.passed).count();
    let failed = results.len() - passed;

    // Step 3: Generate overall summary
    let summary_prompt = format!(
        "{}/{} robots passed the acceptance test for: \"{}\". \
         Key issues: {}. \
         Write a 2-sentence executive summary suitable for a non-technical manager.",
        passed, results.len(), task,
        results.iter()
            .filter(|r| !r.passed)
            .map(|r| format!("{}: {}", r.robot_id, r.details))
            .collect::<Vec<_>>()
            .join("; ")
    );

    let summary = mgr.infer(provider, InferRequest::simple(summary_prompt)).await
        .unwrap_or_else(|_| format!("{}/{} robots passed.", passed, results.len()));

    Ok(AcceptanceReport {
        task: task.to_string(),
        total_robots: results.len(),
        passed,
        failed,
        results,
        summary,
    })
}

/// Raw observable data from one robot, provided by the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobotObservation {
    pub robot_id: String,
    /// ROS topics echoed during the test window
    pub topic_data: serde_json::Value,
    /// System metrics during test
    pub cpu_percent: f64,
    pub error_logs: Vec<String>,
    /// Measured task cycle time (if available from task tracking)
    pub cycle_time_secs: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pass_rate_calculation() {
        let report = AcceptanceReport {
            task: "test".into(),
            total_robots: 4,
            passed: 3,
            failed: 1,
            results: vec![],
            summary: "".into(),
        };
        assert!((report.pass_rate() - 75.0).abs() < 0.01);
    }

    #[test]
    fn test_zero_robots() {
        let report = AcceptanceReport {
            task: "test".into(),
            total_robots: 0,
            passed: 0,
            failed: 0,
            results: vec![],
            summary: "".into(),
        };
        assert_eq!(report.pass_rate(), 0.0);
    }
}
