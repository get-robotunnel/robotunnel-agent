<div align="center">

# RoboTunnel Agent

**Secure remote debug access for ROS 2 robots behind NAT**

*Open-source robot-side agent for stable connectivity, native ROS tooling, and supportable field diagnostics.*

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust 1.75+](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#)
[![Version](https://img.shields.io/badge/version-0.3.0-informational.svg)](#)

</div>

---

RoboTunnel Agent is a lightweight, open-source Rust agent that runs on your robot. In `v0.3.0`, its primary job is to keep ROS 2 robots reachable for remote debugging and basic diagnosis when the robot is remote, behind NAT, or sitting on a weak network where SSH is brittle. LLM API keys are stored **encrypted on the robot only** and never transmitted to any server.

`v0.3.0` is currently in private beta. Invited users can use the hosted product free during the beta period. Invited beta users become Founding Developers and will receive a lifetime discount when paid plans launch. The current packaging direction after beta is active robots plus relay-heavy connection usage, while the agent remains open source.

## What v0.3.0 Officially Means

This first formal release has a narrow boundary. Inside that boundary, we aim to be dependable. Outside it, treat behavior as roadmap or best-effort beta.

- **Current launch promise**: Secure remote debug access for ROS 2 robots behind NAT.
- **Documented support matrix**: Ubuntu 20.04+ / Debian 11+ on the robot side, with ROS 2 Humble / Iron / Jazzy.
- **Golden path**: Install with `scripts/install-agent.sh`, verify with `robotunnel list`, start a live session with `robotunnel connect` or `robotunnel debug ...`, then run your native ROS tools.
- **Stable demo workflows**: Bring one robot online, hold a remote session, run first-line ROS diagnostics, and follow the documented support flow when WebRTC cannot establish cleanly.
- **Explicit non-promises**: Self-hosted platform, non-Linux robot targets, generic IoT coverage, and fully mature fleet operations are not formal `v0.3.0` claims.

## Who This Is For

RoboTunnel `v0.3.0` is built for a sharp ICP, not for every edge device team:

- Teams with real field robots, not just lab simulations
- ROS 2 in the daily workflow
- Robots behind NAT or on unstable remote networks
- Frequent remote debugging and first-line diagnosis needs
- Small teams without time to maintain their own VPN / bastion / WireGuard stack

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

**Connection strategy**: On-demand WebRTC bootstrap triggered by the platform when a CLI client connects. Direct P2P via STUN when possible (no relay, zero server cost). TURN relay only as fallback. Always falls back to TCP tunnel if WebRTC fails. Resources are automatically released via `WebRtcTeardown` signals when clients disconnect.

---

## Why Start With Remote Debugging?

The robot developer's daily reality is narrower and more painful than a big platform slogan suggests: your robot is behind NAT, the onsite link is unreliable, SSH drops at the worst moment, and your ROS tools stop being useful exactly when you need them most.

RoboTunnel starts by making that path more trustworthy:

| Workflow | Without RoboTunnel | With RoboTunnel |
|---|---|---|
| **Bring a remote robot online** | VPN setup, port rules, manual bastions | One documented install path and hosted trust bootstrap |
| **Hold a debug session** | SSH reconnect loops over weak WAN | Adaptive encrypted path with STUN first and fallback when needed |
| **Use native ROS tools** | Custom wrappers or fragile ad hoc forwarding | `ros2` tools work against the remote robot over the held session |
| **Escalate when links fail** | Guesswork and private support DMs | `webrtc-preflight.sh` plus phase-level connection logging |

Monitoring, fleet operations, and agentic workflows remain the direction of travel, but they are not the primary promise of the first formal release.

---

## Security & Trust (Local-First Design)

We built this for the robotics developer community. Trust is non-negotiable.

- **Ed25519 Handshake**: Every agent-platform connection uses a cryptographic challenge-response. No pre-shared passwords.
- **Explicit trust boundary**: The agent requires an Ed25519 public-key allowlist by default. Development-mode permissive auth must be explicitly enabled.
- **Robot-side execution stays local**: Connectivity is authenticated with Ed25519, and robot logic plus LLM calls stay on-device. The current transport stack uses WebRTC when available and authenticated TCP compatibility paths where needed.
- **Local-first LLM keys**: Your OpenAI, Claude, Gemini, and other API keys are stored in `~/.config/robotunnel/agent.keys`, encrypted with AES-256-GCM using a key derived from your machine ID. **These keys are never sent to our servers — not in transit, not in logs, not ever.** We are architecturally prevented from seeing them because we never receive them.
- **Auditable**: This repository is the complete agent source. No binary blobs, no closed-source components.

## Observability & Tracing (v0.3.x)

RoboTunnel `v0.3.x` introduces cross-host tracing for the connection layer:

- **Unified `bootstrap_id`**: Every connection attempt generates a unique UUID (propagated through signaling and platform logs) to simplify debugging across Agent/Platform/CLI boundaries.
- **Phase-Level Logging**: Agent logs now explicitly report phases (`STUN_START`, `SIGNAL_WS_CONNECT`, `OFFER_SENT`, `DATACHANNEL_OPEN`, etc.).
- **On-Demand Lifecycle**: To reduce background noise, WebRTC is only bootstrapped when a CLI is present.
- **Auto-Reconnect Policy**: If a WebRTC link drops while the CLI is still online, the Agent performs one immediate auto-reconnect attempt before falling back to TCP relay.

## Privacy & Data Handling

- RoboTunnel does not persist your command text, LLM conversation content, raw sensor streams, or detailed robot state payloads by default.
- The platform keeps only minimal operational metadata required for service operation, such as connection timing, invocation type, delivery metadata, and transport usage.
- Discord and routing flows may transiently process content in order to execute requests, but the intended product stance is not to store that content as durable conversation history.

> **For the skeptical developer**: Look at `crates/rt-llm/src/keystore.rs`. The LLM provider call is made directly from the agent process to the provider's API. The Platform Gateway is only in the path for session management, not inference.

---

## Prerequisites

| Requirement | Version |
|---|---|
| OS | Ubuntu 20.04+ / Debian 11+ |
| Rust | 1.75+ (stable, only if build-from-source fallback is used) |
| ROS 2 *(optional)* | Humble / Iron / Jazzy |

> **Note**: Linux is required for native ROS 2 integration. The agent compiles on macOS for development, but ROS 2 observation/projection features require a sourced ROS 2 environment.

---

## Quick Start (5 Minutes to First Connection)

### Step 1 — Get your token

RoboTunnel is currently in private beta. Email [russellshe@gmail.com](mailto:russellshe@gmail.com) to receive your `RT_KEY`. Mention what you're building — we currently prioritize teams with ROS 2 robots and real remote-debug needs.

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

### Step 3 — Install, register, and launch the agent (on your robot)

```bash
curl -fsSL https://raw.githubusercontent.com/RussellTNY/robotunnel-agent/main/scripts/install-agent.sh -o install-agent.sh
curl -fsSL https://raw.githubusercontent.com/RussellTNY/robotunnel-agent/main/scripts/install-agent.config.example -o install-agent.config

# Edit install-agent.config first:
# - RT_KEY
# - ROBOT_NAME (optional; defaults to hostname)
# - PLATFORM_BASE_URL (optional; only for self-hosted platform)

chmod +x install-agent.sh
./install-agent.sh ./install-agent.config
```

The installer prefers the published GitHub release binary, falls back to `cargo build --release` when needed, registers the robot via `/api/register`, fetches the platform-managed `authorized_keys` bootstrap set, writes `~/.config/robotunnel/agent.toml`, and starts the agent in the background. The script source of truth lives in [`scripts/install-agent.sh`](./scripts/install-agent.sh).
By default the installer writes `server.authorized_keys` automatically from the platform trust bootstrap path. That set includes the platform gateway key and may include registered CLI device keys for the owning user. If you provide `AUTHORIZED_KEYS` in `install-agent.config`, the installer merges them with the platform-provided keys; it only falls back to `server.insecure_allow_any_client=true` if key bootstrap fails entirely.
After startup, the agent periodically refreshes its TCP `authorized_keys` allowlist from the platform using its `robot_api_key`, so newly registered CLI devices can use direct TCP without manual edits to `agent.toml`.
LLM API keys are not required during install. You can add them later on the robot with `robotunnel-agent keys set ...` after the agent is already online.

If you want to force a source-build fallback, still use the installer:

```bash
git clone https://github.com/RussellTNY/robotunnel-agent.git
cd robotunnel-agent
cp ./scripts/install-agent.config.example ./scripts/install-agent.config

# Edit ./scripts/install-agent.config first (RT_KEY required), then:
AGENT_INSTALL_METHOD=build ./scripts/install-agent.sh ./scripts/install-agent.config
```

If you start the binary manually after installation, `robotunnel-agent` now checks `~/.config/robotunnel/agent.toml` automatically before falling back to `/etc/robotunnel/agent.toml`.

On success you'll see:
```
[INFO] RoboTunnel Agent v0.3.0 starting...
[INFO] Ed25519 handshake complete with platform.robotunnel.io
[INFO] Connection established via STUN (direct P2P)
[INFO] Agent ready. Robot ID: robot-abc123
```

The platform can also store robot identity metadata for chat surfaces:

- `name`: primary display name such as `Spot-1`
- `role`: optional short descriptor such as `picker`
- `avatar_url`: optional Discord/web avatar image URL

Discord uses this metadata to present robot-scoped replies as a concrete robot identity instead of a generic service response.

### Step 4 — Verify from your machine

```bash
$ robotunnel init "your_platform_api_key"
✓ Authenticated

$ robotunnel list
ROBOT ID       IP              STATUS      CONNECTION   LAST SEEN
robot-abc123   192.168.1.105   🟢 Online   STUN/P2P     Just now
```

If WebRTC bootstrap fails and you want a fast control-plane check without rebuilding/restarting the agent, run:

```bash
./scripts/webrtc-preflight.sh \
  --api-url "https://api.robotunnel.io" \
  --api-key "rob_xxx" \
  --robot-id "f4ad43ae-cdc5-4946-ae0f-246ddc73044f"
```

This validates:

1. `/api/agent/authorized-keys`
2. `/api/turn-credentials`
3. WebSocket upgrade on `/api/signal/:robot_id?role=agent`

If your robot host has partial or broken IPv6 setup, keep the default IPv4-only ICE gathering. Only enable IPv6 when your network is known-good:

```bash
export RT_WEBRTC_IPV6_ENABLED=true
```

---

## Support Flow When A Robot Will Not Connect

If the robot is not reachable, treat support as part of the product workflow:

1. Verify presence and route hints with `robotunnel list`.
2. Retry with `robotunnel connect <robot>` or the `robotunnel debug ...` path.
3. Run `./scripts/webrtc-preflight.sh` to check the control-plane pieces without restarting the agent.
4. Inspect phase-level logs such as `SIGNAL_WS_CONNECT`, `OFFER_SENT`, `DATACHANNEL_OPEN`, or TCP fallback messages.
5. If needed, send the preflight result plus the relevant phase logs when asking for help.

This is what "supportable connectivity" means in `v0.3.0`: there is a documented next step when a connection fails.

---

## Setting Up LLM Keys

Run these commands on the machine where `robotunnel-agent` is installed, typically the robot itself or an SSH session into that robot. Keys are stored locally, encrypted. The agent calls the provider API directly — your platform subscription does not cover LLM costs; you pay your provider directly with your own key.

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

Provider selection is explicit for fleet/acceptance execution. The platform does not auto-fallback from one provider to another (for example, OpenAI to Kimi). If the selected provider key is missing on the coordinator robot, the platform returns `error_code=llm_key_missing` with a direct remediation hint.

Monitor alert settings are also local to the robot:

```bash
# Persist monitor alert settings on the robot
robotunnel-agent monitor set-alert cpu_threshold=85 notify=platform

# Inspect current monitor settings
robotunnel-agent monitor show
```

The monitor config is stored in `~/.config/robotunnel/monitor.toml`.
`notify=platform` means "send alert events to the platform, and let the platform deliver them to any subscribed targets such as Discord channels or webhooks." `notify=discord` is kept as a compatibility alias. If you need a local-only fallback during development, `notify=webhook webhook_url=...` is still supported.

For normal day-to-day use, remote monitor configuration should go through the structured `system` skill instead of remote shell access:

```bash
robotunnel skill robot-abc123 system config_get --params '{"section":"monitor"}'
robotunnel skill robot-abc123 system config_set --params '{"section":"monitor","settings":{"enabled":true,"cpu_threshold_percent":85,"notify":"platform"}}'
```

## Release Automation

This repo now publishes agent binaries from Git tags through GitHub Actions.

1. Manually update `Cargo.toml` to the release version (there is no auto-bump step in CI).
2. Commit and push that version change.
3. Push a matching tag such as `v0.3.1` (or `v0.3.1-alpha.1`).
4. The `Release Agent` workflow builds and attaches:
   - `robotunnel-agent-linux-amd64`
   - `robotunnel-agent-linux-arm64`
   - `checksums.txt`

If the tag version and `Cargo.toml` version do not match, the workflow fails early.

---

## Built-in Skills (Default, Always Available)

Detailed documentation for all skills can be found at [docs.robotunnel.io/skills](https://robotunnel.io/docs/skills.html).

For `v0.3.0`, treat remote debugging as the primary launch workflow. Monitoring, fleet comparison, and acceptance flows are available, but they are adjacent capabilities rather than the main release promise.

These built-in workflows are provided by the platform. The generic low-level CLI entry point is always:

```bash
robotunnel skill <robot-selector> <skill> <action> --params '{"...":"..."}'
```

You can discover the machine-readable built-in skill contracts from the agent itself:

```bash
robotunnel skill robot-abc123 system capabilities
```

### 1. Host Debug + ROS2 Observe + Visual Projection
Use `host_debug` for host diagnostics, `ros2_observe` for ROS 2 introspection, and `visual_debug` for projection-plane session controls.

```bash
# Primary CLI workflow: open the debug tunnel
robotunnel connect robot-abc123
# Your local ROS tools now see the remote robot
ros2 topic list
ros2 topic echo /joint_states

# Direct skill actions are also available
robotunnel skill robot-abc123 host_debug status
robotunnel skill robot-abc123 host_debug logs --params '{"lines":50}'
robotunnel skill robot-abc123 host_debug shell --params '{"cmd":"uptime"}'
robotunnel skill robot-abc123 ros2_observe topic_stats --params '{"topic":"/scan","window_sec":8}'
robotunnel skill robot-abc123 visual_debug list_profiles
robotunnel skill robot-abc123 visual_debug start --params '{"mode":"foxglove","transport_policy":"tcp_only","profile":"balanced","desired_delay_ms":120}'
```

`host_debug.shell` is disabled by default. Enable it explicitly with `RT_DEBUG_SHELL_ENABLED=true` only in trusted environments.

`visual_debug` in `mode=rviz_vnc` now starts with secure defaults:

- VNC binds to localhost by default (`RT_RVIZ_VNC_LOCALHOST_ONLY=1`).
- Public/non-localhost bind requires explicit auth (`RT_RVIZ_VNC_PASSWORD_FILE` or `RT_RVIZ_VNC_PASSWORD`), unless you explicitly opt into `RT_RVIZ_VNC_ALLOW_NO_PASSWORD=1`.
- The launcher keeps `rviz2`/`x11vnc`/`Xvfb` under one lifecycle and cleans up child processes on stop.

### 2. Proactive Fleet Monitoring (`rt-skill-monitor`)
The agent continuously samples health metrics and pushes alerts to you when anomalies are detected — without you having to ask.

```bash
# Configure local alert policy on the robot
robotunnel-agent monitor set-alert cpu_threshold=85 notify=platform

# Query a one-shot health snapshot from the generic skill entry point
robotunnel skill robot-abc123 monitor status

# Read or update the remote monitor config over the structured config skill
robotunnel skill robot-abc123 system config_get --params '{"section":"monitor"}'
robotunnel skill robot-abc123 system config_set --params '{"section":"monitor","settings":{"cpu_threshold_percent":85,"notify":"platform"}}'

# In Discord, subscribe the current channel to platform-managed alerts
rt alerts here robot-abc123

# Or subscribe a platform-managed webhook target
rt alerts webhook https://example.com/robot-alerts robot-abc123
```

### 3. Fleet State Comparison (`rt-skill-fleet`)
Ask why one robot is behaving differently from the rest. The platform collects health snapshots from the selected robots, then sends the aggregated payload to one coordinator agent for the local LLM step.

```bash
# The CLI keeps a single generic skill entry point.
# The first robot is the coordinator agent that owns the LLM key.
robotunnel skill robot-1 fleet compare \
  --params '{"query":"Why is robot-3 moving slower than the others?","provider":"openai","robot_ids":["robot-1","robot-2","robot-3"]}'
```

If the coordinator robot has no key for the selected provider, CLI and Discord now surface the exact setup command shape:
`robotunnel-agent keys set <provider> <api-key>`.

### 4. Acceptance Testing (`rt-skill-acceptance`)
Describe a task in plain language. The platform gathers current observations from the selected robots, then one coordinator agent decomposes the task and returns a pass/fail report.

```bash
robotunnel skill robot-1 acceptance test \
  --params '{"task":"Confirm all robots can complete a pick-and-place cycle","provider":"openai","robot_ids":["robot-1","robot-2","robot-3"]}'
```

### Discord Commands

If the platform operator has connected the RoboTunnel Discord bot, the same skills are available from Discord after login:

```text
rt login <platform_token>
rt robots
rt status <robot_selector>
rt logs <robot_selector> [lines]
rt shell <robot_selector> <command...>
rt monitor <robot_selector>
rt compare Why is robot-3 moving slower than the others?
rt test Confirm all robots can complete a pick-and-place cycle
rt skill <robot_selector> <skill> <action> {"param":"value"}
rt Why is robot-3 moving slower than the others?
rt Raise the CPU alert threshold on robot-abc123 to 90 percent
rt confirm
rt alerts
rt alerts here [robot_selector|all]
rt alerts webhook <url> [robot_selector|all]
rt alerts off [robot_selector|all]
rt alerts off webhook <url> [robot_selector|all]
```

In Discord DMs you can also just chat naturally without the `rt` prefix. In server channels, mention the bot and then ask normally. The platform-side Discord intent router sees the available robots, the current robot context for that conversation, and the published skill contracts, then emits one structured skill call, one fleet orchestration request, one read-only robot metadata lookup, or one context update. It does not execute arbitrary shell text on its own. Risky actions such as `host_debug.shell` and `system config_set` still require an explicit `rt confirm`.

The Discord interaction model now distinguishes between:

- `platform chat`
- `robot chat`
- `fleet chat`

Use `use <robot_selector>` to focus on one robot, `context` to inspect the current target, and `back` to return to platform chat. Compare/test responses automatically move the conversation into fleet context. In guild channels, RoboTunnel now opens a dedicated thread when a robot or fleet chat starts, so the target-scoped conversation continues there. The platform repository documents this model in `docs/interaction_layer.md`.

Discord replies now include a strong target anchor such as `▶ Spot-1` at the top of each message. If the current target robot is offline, RoboTunnel reports that immediately with the last heartbeat timestamp instead of waiting for a skill timeout. If a fleet-scoped request is issued inside a robot-scoped conversation, RoboTunnel requires confirmation before expanding scope. Approval now works through Discord buttons as well as the text fallback `rt confirm` / `rt cancel`.

---

## Beta & Pricing

RoboTunnel is currently in private beta.

- Invited users are currently free during the beta period.
- Invited beta users become `Founding Developers` and will receive a lifetime discount when paid plans launch.
- The current packaging direction is active robots plus relay-heavy connection usage.
- Pricing is not expected to be seat-based and is not expected to bill per prompt or per message.
- LLM inference is still bring-your-own-key.
- The agent remains open source.

**Contact [russellshe@gmail.com](mailto:russellshe@gmail.com) to join the beta.** Current rollout details are at [robotunnel.io/pricing](https://robotunnel.io/pricing).

---

## Roadmap

| Version | Status | Highlights |
|---|---|---|
| v0.2.0 | ✅ Released | Ed25519 tunnel, host-debug skill baseline, Kimi AI, Go platform |
| v0.2.3 | ✅ Released | WebRTC P2P, multi-LLM local keys, proactive monitoring, fleet compare, acceptance testing |
| v0.3.0 | 🚧 Shipping soon | Formal remote-debug launch scope, support flow, release binaries, hosted trust bootstrap, CLI debug workflows |
| v0.4.x | 📋 Planned | Monitoring, fleet operations, and more agentic workflows built on the same connectivity layer |

---

## Contributing

Issues and PRs welcome. This repository is the robot-side open-source component of RoboTunnel. The hosted platform covers auth, routing, signaling, and operational control-plane functions and is not part of this repository. Self-hosting is not currently a formal `v0.3.0` promise.

If you're building something interesting with robots, we want to hear about it: [russellshe@gmail.com](mailto:russellshe@gmail.com)

---

## License

MIT License. See [LICENSE](./LICENSE) for details.

**RoboTunnel Platform** (the hosted gateway) is a commercial service — see [robotunnel.io](https://robotunnel.io) for terms.
