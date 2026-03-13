#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-}"
API_KEY="${API_KEY:-}"
ROBOT_ID="${ROBOT_ID:-}"

usage() {
  cat <<'USAGE'
Usage:
  webrtc-preflight.sh --api-url <https://api.robotunnel.io> --api-key <rob_xxx> --robot-id <uuid>

Env alternatives:
  API_URL=... API_KEY=... ROBOT_ID=... ./scripts/webrtc-preflight.sh

Checks:
  1) GET /api/agent/authorized-keys (+ X-Robot-API-Key header)
  2) GET /api/turn-credentials?robot_id=... (+ X-Robot-API-Key header)
  3) WebSocket upgrade /api/signal/<robot_id>?role=agent (+ X-Robot-API-Key header)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-url)
      API_URL="${2:-}"
      shift 2
      ;;
    --api-key)
      API_KEY="${2:-}"
      shift 2
      ;;
    --robot-id)
      ROBOT_ID="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${API_URL}" || -z "${API_KEY}" || -z "${ROBOT_ID}" ]]; then
  echo "[ERROR] API_URL/API_KEY/ROBOT_ID are required" >&2
  usage
  exit 1
fi

API_URL="${API_URL%/}"

http_probe() {
  local name="$1"
  local url="$2"

  local body_file
  body_file="$(mktemp)"
  local code
  code="$(
    curl -sS \
      -o "${body_file}" \
      -w "%{http_code}" \
      -H "X-Robot-API-Key: ${API_KEY}" \
      "${url}" || true
  )"

  echo "[CHECK] ${name}"
  echo "  url : ${url}"
  echo "  code: ${code}"

  if [[ "${code}" != "200" ]]; then
    echo "  body: $(head -c 300 "${body_file}")"
    rm -f "${body_file}"
    return 1
  fi

  if command -v jq >/dev/null 2>&1; then
    echo "  body: $(jq -c . < "${body_file}" 2>/dev/null || head -c 300 "${body_file}")"
  else
    echo "  body: $(head -c 300 "${body_file}")"
  fi

  rm -f "${body_file}"
  return 0
}

ws_upgrade_probe() {
  # WebSocket upgrade starts as an HTTP(S) request with Upgrade headers.
  # curl cannot dial wss:// directly, so probe the HTTPS endpoint instead.
  local ws_url="${API_URL}/api/signal/${ROBOT_ID}?role=agent"

  local body_file header_file
  body_file="$(mktemp)"
  header_file="$(mktemp)"
  local code
  code="$(
    curl --http1.1 -sS \
      -o "${body_file}" \
      -D "${header_file}" \
      -w "%{http_code}" \
      -H "Connection: Upgrade" \
      -H "Upgrade: websocket" \
      -H "Sec-WebSocket-Version: 13" \
      -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
      -H "X-Robot-API-Key: ${API_KEY}" \
      "${ws_url}" || true
  )"

  echo "[CHECK] signaling websocket upgrade"
  echo "  url : ${ws_url}"
  echo "  code: ${code}"
  echo "  hdr : $(head -n 1 "${header_file}" | tr -d '\r')"

  if [[ "${code}" != "101" ]]; then
    echo "  body: $(head -c 300 "${body_file}")"
    rm -f "${body_file}" "${header_file}"
    return 1
  fi

  rm -f "${body_file}" "${header_file}"
  return 0
}

echo "[INFO] running WebRTC preflight..."
http_probe "agent authorized keys" "${API_URL}/api/agent/authorized-keys"
http_probe "turn credentials" "${API_URL}/api/turn-credentials?robot_id=${ROBOT_ID}"
ws_upgrade_probe
echo "[OK] WebRTC control-plane preflight passed"
