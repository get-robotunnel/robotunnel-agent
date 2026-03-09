//! rt-skill-monitor — Proactive health monitoring and anomaly alerting.
//!
//! Runs as a background task inside the agent. Samples system metrics
//! every 30 seconds, detects anomalies via a sliding-window statistical test
//! (z-score > 2.0 = anomaly), and pushes alerts to the platform when detected.
//!
//! The LLM (via `rt-llm`) is used to generate human-readable anomaly explanations
//! before the alert is dispatched — so alerts arrive as natural language, not raw numbers.

use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{info, warn};

use rt_llm::{InferRequest, LlmManager, Provider};

/// A sampled snapshot of key system metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    pub timestamp_unix: u64,
    pub cpu_percent: f64,
    pub mem_used_mb: f64,
    pub mem_total_mb: f64,
    pub disk_used_gb: f64,
    pub disk_total_gb: f64,
    /// Optional: ROS 2 node health (number of active nodes)
    pub ros_node_count: Option<u32>,
}

impl MetricSnapshot {
    /// Collect current system metrics via /proc (Linux).
    pub fn collect() -> Result<Self> {
        let cpu = read_cpu_percent()?;
        let (mem_used, mem_total) = read_mem_mb()?;
        let (disk_used, disk_total) = read_disk_gb()?;

        Ok(Self {
            timestamp_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            cpu_percent: cpu,
            mem_used_mb: mem_used,
            mem_total_mb: mem_total,
            disk_used_gb: disk_used,
            disk_total_gb: disk_total,
            ros_node_count: None, // Populated by rt-skill-ros2 if available
        })
    }

    pub fn mem_percent(&self) -> f64 {
        if self.mem_total_mb > 0.0 {
            self.mem_used_mb / self.mem_total_mb * 100.0
        } else {
            0.0
        }
    }
}

/// Configuration for the monitor skill.
#[derive(Clone, Debug)]
pub struct MonitorConfig {
    /// Sampling interval in seconds (default: 30)
    pub sample_interval_secs: u64,
    /// Window size for baseline calculation (default: 20 samples = 10 min)
    pub window_size: usize,
    /// Z-score threshold for anomaly detection (default: 2.0)
    pub anomaly_z_threshold: f64,
    /// Platform alert webhook URL (POST JSON)
    pub alert_webhook_url: Option<String>,
    /// Platform API base URL for managed alert delivery
    pub platform_api_url: Option<String>,
    /// Robot API key used to authenticate alert delivery with the platform
    pub robot_api_key: Option<String>,
    /// Alert delivery target: log, webhook, discord
    pub notify_target: String,
    /// LLM provider for alert explanation (optional)
    pub llm_provider: Option<Provider>,
    /// Robot ID to include in alerts
    pub robot_id: String,
    /// Optional absolute CPU alert threshold
    pub cpu_threshold_percent: Option<f64>,
    /// Optional absolute memory alert threshold
    pub mem_threshold_percent: Option<f64>,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            sample_interval_secs: 30,
            window_size: 20,
            anomaly_z_threshold: 2.0,
            alert_webhook_url: None,
            platform_api_url: None,
            robot_api_key: None,
            notify_target: "log".to_string(),
            llm_provider: None,
            robot_id: "unknown".to_string(),
            cpu_threshold_percent: None,
            mem_threshold_percent: None,
        }
    }
}

/// Alert payload sent to the platform.
#[derive(Debug, Serialize)]
pub struct MonitorAlert {
    pub robot_id: String,
    pub alert_type: String,
    pub metric: String,
    pub value: f64,
    pub baseline_mean: f64,
    pub baseline_stddev: f64,
    pub explanation: String,
    pub timestamp_unix: u64,
}

struct PendingAlert {
    alert_type: String,
    metric: String,
    value: f64,
    baseline_mean: f64,
    baseline_stddev: f64,
}

/// The monitoring background service.
pub struct MonitorService {
    config: MonitorConfig,
    window: VecDeque<MetricSnapshot>,
}

impl MonitorService {
    pub fn new(config: MonitorConfig) -> Self {
        Self {
            config,
            window: VecDeque::new(),
        }
    }

    /// Run the monitor loop. Blocks until `shutdown_rx` fires.
    pub async fn run(mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = interval(Duration::from_secs(self.config.sample_interval_secs));
        info!(
            "MonitorService started (interval={}s, window={}, z={}, notify={})",
            self.config.sample_interval_secs,
            self.config.window_size,
            self.config.anomaly_z_threshold,
            self.config.notify_target,
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.tick().await {
                        warn!("monitor tick error: {}", e);
                    }
                }
                Ok(_) = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("MonitorService shutting down");
                        break;
                    }
                }
            }
        }
    }

    async fn tick(&mut self) -> Result<()> {
        let snap = MetricSnapshot::collect()?;

        // Detect anomalies against the current baseline window
        let alerts = self.detect_anomalies(&snap);

        // Slide the window
        self.window.push_back(snap.clone());
        if self.window.len() > self.config.window_size {
            self.window.pop_front();
        }

        // If we don't have enough baseline samples yet, skip alerting
        if self.window.len() < 5 {
            return Ok(());
        }

        for pending in alerts {
            let explanation = self
                .explain_anomaly(
                    &pending.metric,
                    pending.value,
                    pending.baseline_mean,
                    pending.baseline_stddev,
                    &snap,
                )
                .await;
            let alert = MonitorAlert {
                robot_id: self.config.robot_id.clone(),
                alert_type: pending.alert_type,
                metric: pending.metric.clone(),
                value: pending.value,
                baseline_mean: pending.baseline_mean,
                baseline_stddev: pending.baseline_stddev,
                explanation,
                timestamp_unix: snap.timestamp_unix,
            };
            warn!(
                "[ALERT] {}: {} = {:.1} (baseline {:.1} ± {:.1}): {}",
                alert.robot_id,
                alert.metric,
                alert.value,
                alert.baseline_mean,
                alert.baseline_stddev,
                alert.explanation
            );
            self.dispatch_alert(alert).await;
        }

        Ok(())
    }

    fn detect_anomalies(&self, snap: &MetricSnapshot) -> Vec<PendingAlert> {
        let cpus: Vec<f64> = self.window.iter().map(|s| s.cpu_percent).collect();
        let mems: Vec<f64> = self.window.iter().map(|s| s.mem_percent()).collect();

        let mut anomalies = Vec::new();
        for (name, samples, current) in [
            ("cpu_percent", &cpus, snap.cpu_percent),
            ("mem_percent", &mems, snap.mem_percent()),
        ] {
            if let Some((mean, stddev)) = mean_stddev(samples) {
                if stddev > 0.5 {
                    let z = (current - mean) / stddev;
                    if z.abs() > self.config.anomaly_z_threshold {
                        anomalies.push(PendingAlert {
                            alert_type: "anomaly".to_string(),
                            metric: name.to_string(),
                            value: current,
                            baseline_mean: mean,
                            baseline_stddev: stddev,
                        });
                    }
                }
            }
        }

        self.push_threshold_alert(
            &mut anomalies,
            "cpu_percent",
            snap.cpu_percent,
            self.config.cpu_threshold_percent,
        );
        self.push_threshold_alert(
            &mut anomalies,
            "mem_percent",
            snap.mem_percent(),
            self.config.mem_threshold_percent,
        );
        anomalies
    }

    fn push_threshold_alert(
        &self,
        alerts: &mut Vec<PendingAlert>,
        metric: &str,
        value: f64,
        threshold: Option<f64>,
    ) {
        let Some(limit) = threshold else {
            return;
        };
        if value < limit {
            return;
        }
        if alerts.iter().any(|alert| alert.metric == metric) {
            return;
        }

        alerts.push(PendingAlert {
            alert_type: "threshold".to_string(),
            metric: metric.to_string(),
            value,
            baseline_mean: limit,
            baseline_stddev: 0.0,
        });
    }

    /// Use LLM to explain the anomaly in natural language, or fall back to a template.
    async fn explain_anomaly(
        &self,
        metric: &str,
        value: f64,
        mean: f64,
        stddev: f64,
        snap: &MetricSnapshot,
    ) -> String {
        if let Some(provider) = &self.config.llm_provider {
            if let Ok(mgr) = LlmManager::open() {
                let prompt = format!(
                    "Robot '{}' anomaly detected. Metric '{}' = {:.1} (baseline mean: {:.1}, stddev: {:.1}). \
                     System snapshot: CPU={:.1}%, RAM={:.0}/{:.0}MB, Disk={:.1}/{:.1}GB. \
                     In 1-2 sentences, explain what might be wrong and suggest a next step.",
                    self.config.robot_id, metric, value, mean, stddev,
                    snap.cpu_percent, snap.mem_used_mb, snap.mem_total_mb,
                    snap.disk_used_gb, snap.disk_total_gb
                );
                let req = InferRequest::simple(prompt);
                if let Ok(explanation) = mgr.infer(provider, req).await {
                    return explanation;
                }
            }
        }
        // Fallback template
        format!(
            "{} spiked to {:.1} (baseline {:.1} ± {:.1}). Check for runaway processes or resource exhaustion.",
            metric, value, mean, stddev
        )
    }

    async fn dispatch_alert(&self, alert: MonitorAlert) {
        match self.config.notify_target.as_str() {
            "log" => {}
            "webhook" => {
                if let Some(url) = &self.config.alert_webhook_url {
                    let client = reqwest::Client::new();
                    if let Err(e) = client.post(url).json(&alert).send().await {
                        warn!("alert dispatch failed: {}", e);
                    }
                } else {
                    warn!("webhook alert requested but no alert_webhook_url configured");
                }
            }
            "platform" | "discord" => {
                if let (Some(api_url), Some(api_key)) =
                    (&self.config.platform_api_url, &self.config.robot_api_key)
                {
                    let client = reqwest::Client::new();
                    let body = serde_json::json!({
                        "api_key": api_key,
                        "notify_target": "discord",
                        "alert_type": alert.alert_type,
                        "metric": alert.metric,
                        "value": alert.value,
                        "baseline_mean": alert.baseline_mean,
                        "baseline_stddev": alert.baseline_stddev,
                        "explanation": alert.explanation,
                        "timestamp_unix": alert.timestamp_unix,
                    });
                    let endpoint = format!("{}/api/alerts", api_url.trim_end_matches('/'));
                    if let Err(e) = client.post(endpoint).json(&body).send().await {
                        warn!("platform discord alert dispatch failed: {}", e);
                    }
                } else {
                    warn!("discord alert requested but platform_api_url/api_key is not configured");
                }
            }
            other => warn!("unknown notify_target '{}'", other),
        }
    }
}

#[cfg(test)]
impl MonitorService {
    fn detect_for_test(&self, snap: &MetricSnapshot) -> Vec<(String, String)> {
        self.detect_anomalies(snap)
            .into_iter()
            .map(|alert| (alert.alert_type, alert.metric))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(cpu: f64, mem_percent: f64) -> MetricSnapshot {
        MetricSnapshot {
            timestamp_unix: 1,
            cpu_percent: cpu,
            mem_used_mb: mem_percent,
            mem_total_mb: 100.0,
            disk_used_gb: 10.0,
            disk_total_gb: 100.0,
            ros_node_count: None,
        }
    }

    #[test]
    fn threshold_alerts_fire_without_baseline() {
        let service = MonitorService {
            config: MonitorConfig {
                cpu_threshold_percent: Some(80.0),
                ..MonitorConfig::default()
            },
            window: VecDeque::new(),
        };

        let alerts = service.detect_for_test(&sample(90.0, 50.0));
        assert!(alerts.contains(&(String::from("threshold"), String::from("cpu_percent"))));
    }

    #[test]
    fn threshold_does_not_duplicate_anomaly_metric() {
        let mut service = MonitorService {
            config: MonitorConfig {
                cpu_threshold_percent: Some(80.0),
                ..MonitorConfig::default()
            },
            window: VecDeque::from(vec![
                sample(10.0, 10.0),
                sample(10.0, 10.0),
                sample(10.0, 10.0),
                sample(10.0, 10.0),
                sample(10.0, 10.0),
            ]),
        };

        let alerts = service.detect_for_test(&sample(90.0, 10.0));
        let cpu_alerts = alerts
            .into_iter()
            .filter(|(_, metric)| metric == "cpu_percent")
            .count();
        assert_eq!(cpu_alerts, 1);
    }
}
// ---------------------------------------------------------------------------
// /proc readers (Linux)
// ---------------------------------------------------------------------------

fn read_cpu_percent() -> Result<f64> {
    // Quick snapshot: read /proc/stat twice with 100ms sleep, compute diff
    // For simplicity in this implementation, return the idle percentage from one read
    let stat1 = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let line = stat1.lines().next().unwrap_or("");
    let nums: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|s| s.parse().ok())
        .collect();
    if nums.len() < 4 {
        return Ok(0.0);
    }
    let total: u64 = nums.iter().sum();
    let idle = nums.get(3).copied().unwrap_or(0);
    if total == 0 {
        return Ok(0.0);
    }
    Ok(100.0 - (idle as f64 / total as f64 * 100.0))
}

fn read_mem_mb() -> Result<(f64, f64)> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let mut total = 0u64;
    let mut available = 0u64;
    for line in meminfo.lines() {
        if line.starts_with("MemTotal:") {
            total = parse_kb(line);
        } else if line.starts_with("MemAvailable:") {
            available = parse_kb(line);
        }
    }
    let used = total.saturating_sub(available);
    Ok((used as f64 / 1024.0, total as f64 / 1024.0))
}

fn parse_kb(line: &str) -> u64 {
    line.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn read_disk_gb() -> Result<(f64, f64)> {
    // Use statvfs on the root mount
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;
        let path = CString::new("/").unwrap();
        let mut stat: MaybeUninit<libc::statvfs> = MaybeUninit::uninit();
        let ret = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
        if ret == 0 {
            let s = unsafe { stat.assume_init() };
            let total = (s.f_blocks * s.f_frsize) as f64 / 1e9;
            let avail = (s.f_bavail * s.f_frsize) as f64 / 1e9;
            return Ok((total - avail, total));
        }
    }
    Ok((0.0, 0.0))
}

fn mean_stddev(data: &[f64]) -> Option<(f64, f64)> {
    if data.is_empty() {
        return None;
    }
    let mean = data.iter().sum::<f64>() / data.len() as f64;
    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / data.len() as f64;
    Some((mean, variance.sqrt()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mean_stddev() {
        let data = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let (mean, stddev) = mean_stddev(&data).unwrap();
        assert!((mean - 30.0).abs() < 0.001);
        assert!(stddev > 0.0);
    }

    #[test]
    fn test_anomaly_detection_triggers_on_spike() {
        let mut config = MonitorConfig::default();
        config.anomaly_z_threshold = 2.0;
        let mut svc = MonitorService::new(config);
        // Build a baseline of stable CPU readings
        for i in 0..10 {
            svc.window.push_back(MetricSnapshot {
                timestamp_unix: i,
                cpu_percent: 20.0 + (i as f64 * 0.1),
                mem_used_mb: 400.0,
                mem_total_mb: 1000.0,
                disk_used_gb: 5.0,
                disk_total_gb: 15.0,
                ros_node_count: None,
            });
        }
        // Spike: CPU = 90%
        let spike = MetricSnapshot {
            timestamp_unix: 100,
            cpu_percent: 90.0,
            mem_used_mb: 400.0,
            mem_total_mb: 1000.0,
            disk_used_gb: 5.0,
            disk_total_gb: 15.0,
            ros_node_count: None,
        };
        let anomalies = svc.detect_anomalies(&spike);
        assert!(!anomalies.is_empty(), "Should detect CPU spike as anomaly");
        assert_eq!(anomalies[0].metric, "cpu_percent");
    }
}
