use std::env;
use std::path::PathBuf;

const KNOWN_ROS_DISTROS: &[&str] = &["jazzy", "humble", "iron", "rolling", "galactic", "foxy"];

pub fn ros_setup_script_path() -> Option<PathBuf> {
    for raw in [
        env::var("RT_ROS_SETUP").ok(),
        env::var("ROS_SETUP").ok(),
        env::var("AMENT_PREFIX_PATH")
            .ok()
            .and_then(|value| {
                value
                    .split(':')
                    .find(|part| !part.trim().is_empty())
                    .map(str::to_string)
            })
            .map(|prefix| format!("{}/setup.bash", prefix.trim_end_matches('/'))),
    ]
    .into_iter()
    .flatten()
    {
        let candidate = PathBuf::from(raw.trim());
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    if let Ok(distro) = env::var("ROS_DISTRO") {
        let candidate = PathBuf::from(format!("/opt/ros/{}/setup.bash", distro.trim()));
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    for distro in KNOWN_ROS_DISTROS {
        let candidate = PathBuf::from(format!("/opt/ros/{}/setup.bash", distro));
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    None
}

pub fn wrap_ros_shell(command: &str) -> String {
    let command = command.trim();
    if command.is_empty() {
        return String::new();
    }

    match ros_setup_script_path() {
        Some(setup) => format!(
            ". {} >/dev/null 2>&1 && {}",
            shell_quote(&setup.to_string_lossy()),
            command
        ),
        None => command.to_string(),
    }
}

pub fn ros2_shell_command(args: &[&str]) -> String {
    let mut command = String::from("exec ros2");
    for arg in args {
        command.push(' ');
        command.push_str(&shell_quote(arg));
    }
    wrap_ros_shell(&command)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::{ros2_shell_command, wrap_ros_shell};

    #[test]
    fn ros2_shell_command_quotes_arguments() {
        let command = ros2_shell_command(&["topic", "echo", "/te'st"]);
        assert!(command.contains("exec ros2 'topic' 'echo' '/te'\"'\"'st'"));
    }

    #[test]
    fn wrap_ros_shell_keeps_original_command_without_setup() {
        std::env::remove_var("RT_ROS_SETUP");
        std::env::remove_var("ROS_SETUP");
        std::env::remove_var("ROS_DISTRO");
        std::env::remove_var("AMENT_PREFIX_PATH");
        let command = wrap_ros_shell("exec ros2 topic list");
        assert!(!command.is_empty());
    }
}
