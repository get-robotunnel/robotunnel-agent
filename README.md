<div align="center">

# RoboTunnel Agent

**The Physical World API Layer**

*Turn your robots and IoT devices into LLM-callable functions.*

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.75+](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![Version](https://img.shields.io/badge/version-0.3.0-informational.svg)](#)

</div>

---

RoboTunnel Agent is a lightweight, open-source Rust agent that runs on your robot. It maintains secure connectivity to the RoboTunnel Platform and executes robot-side skills for remote debugging, proactive fleet monitoring, and natural-language control. LLM API keys are stored **encrypted on the robot only** and never transmitted to any server.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Developer / Operator                    │
│              CLI ·  Discord Bot ·  Custom Webhook            │
└─────────────────────────┬────────────────────────────────────┘
                          │  REST / WebSocket
                          ▼
              ┌───────────────────────┐
              │  RoboTunnel Platform  │  ← Go Gateway (hosted)
              │    (robotunnel.io)    │
              └───────────┬───────────┘
                          │  Ed25519-authenticated TCP / WebRTC
          ┌───────────────┼───────────────────┐
          ▼               ▼                   ▼
   ┌─────────────┐ ┌─────────────┐   ┌─────────────┐
   │  Robot #1   │ │  Robot #2   │   │  Robot #N   │
   │             │ │             │   │             │
   │  RT Agent   │ │  RT Agent   │   │  RT Agent   │
   │  (this repo)│ │             │   │             │
   │  ┌────────┐ │ └─────────────┘   └─────────────┘
   │  │ Skills │ │
   │  │ LLM    │ │  ← Your API keys, stored locally.
   │  │ Keys   │ │    Encrypted at rest. Never leave this machine.
   │  └────────┘ │
   │  ROS 2 Node │
   └─────────────┘
```

**Connection strategy**: Direct P2P via STUN when possible (no relay, zero server cost). TURN relay only as fallback. Always falls back to TCP tunnel if WebRTC fails. No single point of failure.

---

## Why RoboTunnel Agent?

The robot developer's daily reality: your robot is behind a NAT, SSH is fragile under WAN, and there's no good answer for "why is robot #3 acting weird?" from the field.

RoboTunnel solves four scenarios that are broken in today's CLI-first world:

| Scenario | Without RoboTunnel | With RoboTunnel |
|---|---|---|
| **Remote Debug** | SSH → pray the connection holds | Persistent encrypted tunnel, ROS tools work natively |
| **Fleet Monitoring** | Poll each robot manually, miss issues overnight | LLM proactively alerts you when anomalies are detected |
| **Fleet Comparison** | Open 5 terminals, diff by hand | "Why is robot #3 different?" → natural language answer |
| **Acceptance Testing** | Write test scripts, technical knowledge required | "Confirm all robots can complete a pick task" → pass/fail report |

---

## Security & Trust (Local-First Design)

We built this for the robotics developer community. Trust is non-negotiable.

- **Ed25519 Handshake**: Every agent-platform connection uses a cryptographic challenge-response. No pre-shared passwords.
- **Explicit trust boundary**: The agent requires an Ed25519 public-key allowlist by default. Development-mode permissive auth must be explicitly enabled.
- **Robot-side execution stays local**: Connectivity is authenticated with Ed25519, and robot logic plus LLM calls stay on-device. The current transport stack uses WebRTC when available and authenticated TCP compatibility paths where needed.
- **Local-first LLM keys**: Your OpenAI, Claude, Gemini, and other API keys are stored in `~/.config/robotunnel/agent.keys`, encrypted with AES-256-GCM using a key derived from your machine ID. **These keys are never sent to our servers — not in transit, not in logs, not ever.** We are architecturally prevented from seeing them because we never receive them.
- **Auditable**: This repository is the complete agent source. No binary blobs, no closed-source components.

> **For the skeptical developer**: Look at `crates/rt-llm/src/keystore.rs`. The LLM provider call is made directly from the agent process to the provider's API. The Platform Gateway is only in the path for session management, not inference.

---

## Prerequisites

| Requirement | Version |
|---|---|
| OS | Ubuntu 20.04+ / Debian 11+ |
| Rust | 1.75+ (stable) |
| ROS 2 *(optional)* | Humble / Iron / Jazzy |

> **Note**: Linux is required for native ROS 2 integration. The agent compiles on macOS for development, but `rt-skill-ros2` requires a sourced ROS 2 environment.

---

## Quick Start (5 Minutes to First Connection)

### Step 1 — Get your token

RoboTunnel is currently in developer-invite phase. Email [russellshe@gmail.com](mailto:russellshe@gmail.com) to receive your `RT_KEY`. Mention what you're building — we prioritize robotics and IoT developers.

### Step 2 — Install the CLI (your development machine)

Download the pre-compiled binary from [Releases](https://github.com/RussellTNY/robotunnel/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/RussellTNY/robotunnel/releases/latest/download/robotunnel-darwin-arm64 -o robotunnel
chmod +x robotunnel && sudo mv robotunnel /usr/local/bin/

# Linux (amd64)
curl -L https://github.com/RussellTNY/robotunnel/releases/latest/download/robotunnel-linux-amd64 -o robotunnel
chmod +x robotunnel && sudo mv robotunnel /usr/local/bin/
```

### Step 3 — Build and launch the agent (on your robot)

```bash
git clone https://github.com/RussellTNY/robotunnel-agent.git
cd robotunnel-agent
cargo build --release

# Start (replace with your actual key)
RT_KEY="rt_your_key_here" ./target/release/robotunnel-agent
```

On success you'll see:
```
[INFO] RoboTunnel Agent v0.3.0 starting...
[INFO] Ed25519 handshake complete with platform.robotunnel.io
[INFO] Connection established via STUN (direct P2P)
[INFO] Agent ready. Robot ID: robot-abc123
```

### Step 4 — Verify from your machine

```bash
$ robotunnel init "your_platform_api_key"
✓ Authenticated

$ robotunnel list
ROBOT ID       IP              STATUS      CONNECTION   LAST SEEN
robot-abc123   192.168.1.105   🟢 Online   STUN/P2P     Just now
```

---

## Setting Up LLM Keys

The agent supports 8 LLM providers. Keys are stored locally, encrypted. The agent calls the provider API directly — your platform subscription does not cover LLM costs; you pay your provider directly with your own key.

```bash
# Set a key (stored encrypted on this machine only)
robotunnel-agent keys set openai     "sk-..."
robotunnel-agent keys set claude     "sk-ant-..."
robotunnel-agent keys set gemini     "AIza..."
robotunnel-agent keys set grok       "xai-..."
robotunnel-agent keys set deepseek   "sk-..."
robotunnel-agent keys set minimax    "..."
robotunnel-agent keys set kimi       "..."
robotunnel-agent keys set qwen       "sk-..."

# View configured providers (keys are masked)
robotunnel-agent keys list
# Provider    Status      Last Used
# openai      ✓ Set       2 hours ago
# claude      ✓ Set       Yesterday
# gemini      ✗ Not set   —

# Remove a key
robotunnel-agent keys remove openai
```

Keys are stored in `~/.config/robotunnel/agent.keys`. The file is AES-256-GCM encrypted using a key derived from your machine's hardware ID. Even if someone copies this file to another machine, it cannot be decrypted.

---

## Built-in Skills (Default, Always Available)

These four scenarios are provided by the platform and require no additional configuration:

You can discover the machine-readable built-in skill contracts from the agent itself:

```bash
robotunnel skill robot-abc123 system capabilities
```

### 1. Remote Debug (`rt-skill-debug`)
Stream ROS topics, view system logs, inspect processes — everything your local `ros2 topic echo` does, but over WAN with a stable persistent connection.

```bash
robotunnel connect robot-abc123
# Your local ROS tools now see the remote robot
ros2 topic list
ros2 topic echo /joint_states
```

### 2. Proactive Fleet Monitoring (`rt-skill-monitor`)
The agent continuously samples health metrics and pushes alerts to you when anomalies are detected — without you having to ask.

```bash
# Configure alert threshold and destination
robotunnel-agent monitor set-alert cpu_threshold=85 notify=discord

# Example alert you receive automatically:
# [RoboTunnel Alert] Robot #2 — CPU spike detected (92% for 3min).
# Baseline: 35%. Possible cause: runaway process. Process tree attached.
```

### 3. Fleet State Comparison (`rt-skill-fleet`)
Ask why one robot is behaving differently from the rest. The agent collects state snapshots from all connected robots and generates a natural-language diff.

```bash
robotunnel fleet compare --query "Why is robot-3 moving slower than the others?"
# Analyzing 5 robots...
# Robot-3 anomalies vs fleet average:
#   • CPU: 78% (fleet avg: 34%) — motion_planner process consuming 44%
#   • /cmd_vel publish rate: 8Hz (fleet avg: 20Hz)
#   • Last error: costmap_2d: [WARN] inflation radius exceeds obstacle range
# Likely cause: costmap configuration mismatch. Suggested fix: ...
```

### 4. Acceptance Testing (`rt-skill-acceptance`)
Describe a task in plain language. The system decomposes it into observable checks, runs them across your fleet, and returns a pass/fail report — no scripting required.

```bash
robotunnel fleet test --task "Confirm all robots can complete a pick-and-place cycle"
# Testing 5 robots against task criteria...
# ✓ Robot-1: PASS (cycle time: 4.2s)
# ✓ Robot-2: PASS (cycle time: 4.5s)
# ✗ Robot-3: FAIL — gripper sensor timeout at stage 3
# ✓ Robot-4: PASS (cycle time: 4.1s)
# ✓ Robot-5: PASS (cycle time: 4.3s)
# Result: 4/5 PASS. Report saved to ./acceptance_report_20260302.json
```

---

## Subscription & Pricing

RoboTunnel charges for connection resources only. LLM inference, the agent software, and the open-source code are free.

| Plan | Price | Robots |
|---|---|---|
| Solo Developer | €19/mo | 3 robots |
| Team | €49/mo | 15 robots |

Overage: connections are not cut. You get a 7-day grace period with dashboard/email notification to upgrade or reduce connections.

**Contact [russellshe@gmail.com](mailto:russellshe@gmail.com) to subscribe.** Full pricing details at [robotunnel.io/pricing](https://robotunnel.io/pricing).

---

## Roadmap

| Version | Status | Highlights |
|---|---|---|
| v0.2.0 | ✅ Released | Ed25519 tunnel, debug skill, Kimi AI, Go platform |
| v0.2.3 | ✅ Released | WebRTC P2P, multi-LLM local keys, proactive monitoring, fleet compare, acceptance testing |
| v0.4.x | 📋 Planned | Skill Platform — publish your robot's capabilities for others to discover and use |

---

## Contributing

Issues and PRs welcome. This is the open-source component of RoboTunnel — robot-side only. The platform gateway is closed-source and hosted.

If you're building something interesting with robots, we want to hear about it: [russellshe@gmail.com](mailto:russellshe@gmail.com)

---

## License

MIT License. See [LICENSE](./LICENSE) for details.

**RoboTunnel Platform** (the hosted gateway) is a commercial service — see [robotunnel.io](https://robotunnel.io) for terms.
