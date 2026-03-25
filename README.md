<div align="center">

# RoboTunnel Agent

**ROS 2 remote debugging that actually works.**

*Open-source robot-side agent for local-first debugging, managed debug sessions, and Debug Projection.*

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.75+](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![Version](https://img.shields.io/badge/version-0.3.0-informational.svg)](#)

</div>

---

RoboTunnel Agent is the open-source process that runs on your robot. It keeps the robot reachable for remote debugging, owns the robot-side trust boundary, and hosts the local skill surface used by the CLI and Discord integrations.

For `v0.3.0`, the main job is narrow on purpose: help ROS 2 developers debug real robots behind NAT, on unstable field networks, without forcing them into a brittle SSH + ad hoc port-forwarding workflow.

## Why RoboTunnel

ROS 2 remote debugging usually fails in exactly the moments you need it most. The robot is behind NAT, onsite Wi-Fi is weak, SSH drops, forwarded ports drift out of sync, and the tools you actually want to use, like Foxglove, RViz, `ros2 topic`, and structured robot-side diagnostics, stop being dependable.

RoboTunnel takes a different path. The agent runs on the robot, keeps trust and LLM execution local, and lets you start managed debug sessions from the CLI. Instead of treating remote debugging as raw tunnel plumbing, RoboTunnel adds a Debug Projection layer that adapts robot topics for real-world links while preserving the workflows developers already know.

## Architecture

```text
Developer                            Team
           │                              │
           │ CLI                          │ Discord
           │ init/list/debug              │ status/logs/alerts
           │ connect/skill                │ natural-language ops
           │                              │
           └──────────┬───────────────────┘
                      │
                      ▼
           ┌─────────────────────┐
           │  RoboTunnel Platform│
           │  auth · routing     │
           │  signaling · relay  │
           └──────────┬──────────┘
                      │
              Ed25519 + WebRTC/TCP
                      │
                      ▼
           ┌─────────────────────┐
           │   Robot-side Agent  │
           │                     │
           │  visual_debug       │  ◄──     Debug Projection
           │  ros2_observe       │          (bandwidth-adaptive)
           │  host_debug         │
           │  monitor            │
           │                     │
           │  LLM keys local     │
           └─────────────────────┘
                      │
           (CLI debug session only)
                      │
           Foxglove · RViz · ROS CLI
```

## Quick Start

### Step 1. Install the CLI on your development machine

```bash
curl -fsSL https://downloads.robotunnel.io/install.sh -o install-robotunnel.sh
bash install-robotunnel.sh
```

Authenticate once with your platform token:

```bash
robotunnel init "<platform-token>"
```

### Step 2. Install the agent on the robot

```bash
curl -fsSL https://raw.githubusercontent.com/RussellTNY/robotunnel-agent/main/scripts/install-agent.sh -o install-agent.sh
curl -fsSL https://raw.githubusercontent.com/RussellTNY/robotunnel-agent/main/scripts/install-agent.config.example -o install-agent.config

# Edit install-agent.config:
# - RT_KEY
# - ROBOT_NAME (optional; defaults to hostname)
# - PLATFORM_BASE_URL (optional; only for self-hosted platform)

chmod +x install-agent.sh
./install-agent.sh ./install-agent.config
```

### Step 3. Start a managed debug session

For `--mode foxglove`, `--launch` opens the Foxglove endpoint for you. If you prefer to attach manually, or you are using a machine without a browser session, run the same command without `--launch`.

```bash
# Verify the robot is online
robotunnel list

# Start a visual debug session
robotunnel debug start <robot> --mode foxglove

# Attach, forward locally, and open the client
robotunnel debug open <robot> --launch
```

At this point your local tools can work against the remote session. `robotunnel connect <robot>` still exists, but it is the expert low-level tunnel path, not the default onboarding flow.

Example output:

```text
$ robotunnel list
ROBOT ID       NAME                      STATUS      CONNECTION   LAST SEEN
21e56bda...    Robot-Lab-01              🟢 Online      tcp         Just now

$ robotunnel debug start Robot-Lab-01 --mode foxglove
Started debug session
  Session ID: dbg_7f3f1d6f
  Mode: foxglove
  Endpoint: available via `robotunnel debug open Robot-Lab-01 --launch`
```

## Debug Projection

Debug Projection is the capability that makes RoboTunnel different from a generic tunnel or VPN.

Instead of only forwarding a raw connection and hoping the network holds, RoboTunnel can create a managed projection session for remote debugging. The agent samples and adapts robot-side data, applies profile or per-topic policy, and exposes a debug endpoint that works with existing tools such as Foxglove and RViz.

### What it is

- A robot-side projection plane for visual and topic-driven debugging.
- A managed session model exposed through `robotunnel debug ...` and the `visual_debug` skill.
- A way to keep debugging usable on constrained links by projecting the data you need instead of naively forwarding everything.

### What it supports today

Current projection/runtime support is strongest for the common ROS 2 debug path:

- `sensor_msgs/msg/Image`
  - Resize / compression-oriented policy via built-in profiles and topic policy overrides.
- `sensor_msgs/msg/PointCloud2`
  - Downsampling-oriented policy such as stride / voxel-style reduction.
- `sensor_msgs/msg/LaserScan`
  - Stride / lower-rate projection for constrained links.
- `tf2_msgs/msg/TFMessage`
  - Forced passthrough guardrail to avoid breaking transform chains.
- Other topics
  - Fall back to relay or throttled handling when a transform is not explicitly supported.

### Built-in profiles and policy

RoboTunnel ships with session profiles for common remote-debug situations:

- `balanced`
- `lidar_low_bw`
- `vision_low_bw`
- `stats_only`
- `compressed_passthrough`
- `compressed_resize`

Use profiles for the fast path:

```bash
robotunnel debug start <robot> \
  --mode foxglove \
  --profile lidar_low_bw \
  --topic /lidar/points \
  --transport tcp_only
```

Override per-topic policy when you need finer control:

```bash
robotunnel debug start <robot> \
  --mode foxglove \
  --topic /camera/image_raw \
  --topic-policy-file ./topic_policy.json
```

Example policy:

```json
{
  "/camera/image_raw": {
    "image_scale": 0.5,
    "max_fps": 8
  },
  "/lidar/points": {
    "point_stride": 4,
    "voxel": 0.10
  },
  "*": {
    "max_fps": 10
  }
}
```

### Foxglove and RViz integration

Foxglove is the default visual path:

```bash
robotunnel debug start <robot> --mode foxglove --profile balanced
robotunnel debug open <robot> --launch
```

For RViz-over-VNC sessions:

```bash
robotunnel debug start <robot> --mode rviz_vnc
robotunnel debug open <robot> --endpoint rviz_vnc --launch
```

For inspection without opening a client yet:

```bash
robotunnel debug status <robot>
robotunnel debug profiles <robot>
robotunnel debug stream <robot> --session-id <sid> --topic /scan --follow
```

## Discord Integration

Discord is the operator/team surface, not the primary README path.

The platform bot can help with:

- robot status and logs
- alert delivery
- monitor / compare style workflows
- natural-language requests that route into structured robot skills

What stays CLI-first:

- interactive debug-session opening
- `robotunnel debug open`
- expert `connect` workflows

The intent is simple: Discord is for team interaction and light-weight operations, while the CLI remains the primary surface for hands-on developer debugging.

## Skills

These are the four robot-side skills most developers will touch first:

- `visual_debug`
  - Starts and manages Debug Projection sessions.
- `ros2_observe`
  - Discovers topics, samples data, and inspects ROS 2 state.
- `host_debug`
  - Checks host health, logs, and shell-level diagnostics.
- `monitor`
  - Captures health snapshots and background alert signals.

There are additional built-ins in the broader product surface, but these four explain most of the first developer journey.

## Security

Trust is a product feature, not a footer note.

- **Local LLM keys**: Provider keys are stored on the robot only, encrypted at rest, and used directly by the robot-side agent.
- **Ed25519 authentication**: Connections use cryptographic challenge-response, not shared passwords.
- **Explicit trust boundary**: The agent uses an allowlist model for client keys by default.
- **Open-source robot edge**: This repository is the robot-side code that runs on the machine.
- **Outbound-first connectivity**: The robot initiates the connection; you do not need to expose inbound ports on the robot.

## Supported Today

`v0.3.0` is intentionally narrow.

- Robot OS: Ubuntu 20.04+ / Debian 11+
- ROS 2: Humble / Iron / Jazzy
- Primary promise: ROS 2 remote debugging for robots behind NAT
- Supported onboarding path: hosted RoboTunnel platform
- Expert path still supported: `robotunnel connect`
- Advanced option exists: `PLATFORM_BASE_URL` can be overridden for internal or experimental setups
- Not the current promise: self-hosted platform support, generic IoT coverage, or mature fleet operations

If you want the deeper installation, support-flow, or reference docs, use the public docs at [robotunnel.io/docs](https://robotunnel.io/docs/).

## Roadmap

| Version | Focus |
|---|---|
| `v0.3.0` | Remote debugging that actually works for ROS 2 robots behind NAT |
| `v0.4.x` | Harder Debug Projection sessions, stronger observability, better session quality on weak links |
| `v0.5.x` | Better team workflows through Discord, alerts, and shared operational context |
| `Later` | Fleet-level diagnostics and broader operational workflows after the robot-level debug path is dependable |
