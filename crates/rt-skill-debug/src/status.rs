//! System status query for the Debug Skill.

use rt_core::protocol::{CommandRequest, CommandResponse, CommandStatus};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct SystemStatus {
    hostname: String,
    uptime: String,
    cpu_count: usize,
    memory: MemoryInfo,
    disk: DiskInfo,
    load_average: String,
}

#[derive(Debug, Serialize)]
struct MemoryInfo {
    total_mb: u64,
    available_mb: u64,
    used_percent: f64,
}

#[derive(Debug, Serialize)]
struct DiskInfo {
    total_gb: f64,
    available_gb: f64,
    used_percent: f64,
}

/// Handle a "status" action.
///
/// Returns system status information including hostname, uptime, CPU, memory, disk.
pub async fn handle(request: CommandRequest) -> CommandResponse {
    match collect_status().await {
        Ok(status) => CommandResponse {
            id: request.id,
            status: CommandStatus::Ok,
            data: Some(serde_json::to_value(status).unwrap_or_default()),
            error: None,
        },
        Err(e) => CommandResponse {
            id: request.id,
            status: CommandStatus::Error,
            data: None,
            error: Some(format!("failed to collect status: {}", e)),
        },
    }
}

async fn collect_status() -> Result<SystemStatus, String> {
    let hostname = read_file_trimmed("/etc/hostname")
        .unwrap_or_else(|| "unknown".to_string());

    let uptime = run_cmd("uptime -p").await.unwrap_or_default();
    let load = run_cmd("cat /proc/loadavg").await.unwrap_or_default();
    let cpu_count = num_cpus();

    let memory = parse_meminfo().unwrap_or(MemoryInfo {
        total_mb: 0,
        available_mb: 0,
        used_percent: 0.0,
    });

    let disk = parse_disk_usage().await.unwrap_or(DiskInfo {
        total_gb: 0.0,
        available_gb: 0.0,
        used_percent: 0.0,
    });

    Ok(SystemStatus {
        hostname,
        uptime: uptime.trim().to_string(),
        cpu_count,
        memory,
        disk,
        load_average: load.trim().to_string(),
    })
}

fn read_file_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn parse_meminfo() -> Option<MemoryInfo> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut total_kb = 0u64;
    let mut available_kb = 0u64;

    for line in content.lines() {
        if let Some(val) = line.strip_prefix("MemTotal:") {
            total_kb = parse_kb_value(val);
        } else if let Some(val) = line.strip_prefix("MemAvailable:") {
            available_kb = parse_kb_value(val);
        }
    }

    let total_mb = total_kb / 1024;
    let available_mb = available_kb / 1024;
    let used_percent = if total_kb > 0 {
        ((total_kb - available_kb) as f64 / total_kb as f64) * 100.0
    } else {
        0.0
    };

    Some(MemoryInfo {
        total_mb,
        available_mb,
        used_percent: (used_percent * 10.0).round() / 10.0,
    })
}

fn parse_kb_value(s: &str) -> u64 {
    s.trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .unwrap_or(0)
}

async fn parse_disk_usage() -> Option<DiskInfo> {
    let output = run_cmd("df -B1 /").await?;
    // Parse `df` output: second line has the values
    let line = output.lines().nth(1)?;
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 5 {
        return None;
    }

    let total: f64 = parts[1].parse().unwrap_or(0.0);
    let used: f64 = parts[2].parse().unwrap_or(0.0);
    let available: f64 = parts[3].parse().unwrap_or(0.0);

    let total_gb = total / 1_073_741_824.0;
    let available_gb = available / 1_073_741_824.0;
    let used_percent = if total > 0.0 {
        (used / total) * 100.0
    } else {
        0.0
    };

    Some(DiskInfo {
        total_gb: (total_gb * 10.0).round() / 10.0,
        available_gb: (available_gb * 10.0).round() / 10.0,
        used_percent: (used_percent * 10.0).round() / 10.0,
    })
}

async fn run_cmd(cmd: &str) -> Option<String> {
    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .await
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        None
    }
}
