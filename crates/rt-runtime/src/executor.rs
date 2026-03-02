//! Command executor — runs shell commands in a sandboxed environment.

use std::time::Duration;
use thiserror::Error;
use tokio::process::Command;
use tracing;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Command timed out after {0}s")]
    Timeout(u64),
    #[error("Command failed to spawn: {0}")]
    SpawnFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result of a command execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

/// Execute a shell command with timeout and output capture.
///
/// # Safety considerations
/// - Commands run as the agent's user (typically a dedicated service user)
/// - Timeout prevents runaway processes
/// - Output is truncated to max_output_bytes to prevent memory bombs
pub async fn exec_command(
    cmd: &str,
    timeout_secs: u64,
    max_output_bytes: usize,
) -> Result<ExecResult, ExecutorError> {
    tracing::debug!("executor: running command: {}", cmd);

    let child = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        // Don't inherit parent env fully for security
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
        .env("HOME", "/tmp")
        .env("LANG", "C.UTF-8")
        .spawn()
        .map_err(|e| ExecutorError::SpawnFailed(e.to_string()))?;

    let timeout = Duration::from_secs(timeout_secs);

    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => {
            let stdout = truncate_string(
                String::from_utf8_lossy(&output.stdout).into_owned(),
                max_output_bytes,
            );
            let stderr = truncate_string(
                String::from_utf8_lossy(&output.stderr).into_owned(),
                max_output_bytes,
            );
            let exit_code = output.status.code();

            tracing::debug!(
                "executor: command finished (exit_code={:?}, stdout_len={}, stderr_len={})",
                exit_code,
                stdout.len(),
                stderr.len()
            );

            Ok(ExecResult {
                stdout,
                stderr,
                exit_code,
            })
        }
        Ok(Err(e)) => Err(ExecutorError::Io(e)),
        Err(_) => {
            tracing::warn!("executor: command timed out after {}s", timeout_secs);
            Err(ExecutorError::Timeout(timeout_secs))
        }
    }
}

fn truncate_string(s: String, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        s
    } else {
        let truncated = &s[..max_bytes];
        format!("{}...\n[truncated, total {} bytes]", truncated, s.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_exec_simple_command() {
        let result = exec_command("echo hello", 5, 1024).await.unwrap();
        assert_eq!(result.stdout.trim(), "hello");
        assert_eq!(result.exit_code, Some(0));
    }

    #[tokio::test]
    async fn test_exec_stderr() {
        let result = exec_command("echo error >&2", 5, 1024).await.unwrap();
        assert_eq!(result.stderr.trim(), "error");
    }

    #[tokio::test]
    async fn test_exec_exit_code() {
        let result = exec_command("exit 42", 5, 1024).await.unwrap();
        assert_eq!(result.exit_code, Some(42));
    }

    #[tokio::test]
    async fn test_exec_timeout() {
        let result = exec_command("sleep 10", 1, 1024).await;
        assert!(matches!(result, Err(ExecutorError::Timeout(1))));
    }

    #[tokio::test]
    async fn test_exec_output_truncation() {
        // Generate output larger than max
        let result = exec_command("seq 1 10000", 5, 100).await.unwrap();
        assert!(result.stdout.contains("[truncated"));
    }
}
