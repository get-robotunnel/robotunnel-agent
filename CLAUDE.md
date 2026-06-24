# CLAUDE.md — RoboTunnel Agent (Robot Operations, robot side)

This repo is the **robot-side runtime** of Robot Operations: a Rust binary that
runs on the robot, opens the tunnel to the platform, and executes skills
(host debug, ROS 2 observe, monitor, fleet, acceptance). Released to Cloudflare
R2 (`downloads.robotunnel.io`) via GitHub Actions.

## Sibling products / where things live

- **`../robot-agent-tunnel`** — the open-source **tunnel** (connection layer). The
  canonical home of the connection crates `rt-core` (tunnel/auth/heartbeat) and
  `rt-webrtc` (STUN→TURN→TCP). This repo currently has its own copy under
  `crates/`; the migration path is to consume them from the tunnel repo as a git
  dependency (apply the same `[patch.crates-io] webrtc-ice` from
  `../robot-agent-tunnel/rust`). Do this as a deliberate, CI-validated change —
  the agent is a deployed binary; don't break its release pipeline.
- **`../robotunnel`** — the Robot Operations **platform** (Go, server side).
- **`../robot-agent-registry`** — open-source registry (separate product).

## Layout

```
src/main.rs           Binary entry: load config, build tunnel server + router, wire skills
crates/rt-core        Connection: TCP tunnel, Ed25519 auth, heartbeat, config  (canonical: ../robot-agent-tunnel)
crates/rt-webrtc      WebRTC P2P: STUN→TURN→TCP fallback + signaling           (canonical: ../robot-agent-tunnel)
crates/rt-runtime     Skill dispatch trait / command router (rt-agent-dispatch)
crates/rt-skill-*     host-debug, ros2-observe, monitor, fleet, acceptance
crates/rt-llm         Local encrypted key storage + multi-provider inference
src/interaction/      Bridge: tunnel/WebRTC frames <-> app command router
vendor/webrtc-ice     Patched ICE fork (Docker/VPN/Tailscale LANs); see [patch.crates-io]
```

## Conventions

- Build: `cargo build --release`. Release matrix (musl x86_64, gnu arm64) in
  `.github/workflows/release.yml` → R2.
- The agent connects to the tunnel via `RT_API_URL` (default
  `https://api.robotunnel.io`, migrating to `https://tunnel.robotunnel.io`).
  Other env: `RT_API_KEY` (robot_api_key), `RT_LISTEN_PORT` (11411),
  `RT_WEBRTC_ENABLED`, `RT_AUTHORIZED_KEYS`. See `crates/rt-core/src/config.rs`.
- The connection crates have **zero** dependency on skill/ops crates (inverted
  dependency tree). Keep it that way — connection logic belongs in the tunnel
  repo, business logic in the skills.

## Tunnel cutover note

When the platform's Phase-2 cutover is live, agents reach the connection
endpoints (`/api/signal`, `/api/agent/connect`, `/api/agent/relay`,
`/api/turn-credentials`) at `tunnel.robotunnel.io`. Existing builds keep working
via the `api.robotunnel.io` Caddy strangler; new builds should default
`RT_API_URL` to the tunnel host. The wire protocol is specified in
`../robot-agent-tunnel/spec/tunnel-protocol.md`.
