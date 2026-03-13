#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${1:-}"
if [[ -n "${CONFIG_FILE}" ]]; then
  if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "[ERROR] config file not found: ${CONFIG_FILE}" >&2
    exit 1
  fi

  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[ERROR] missing command: $1" >&2
    exit 1
  }
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "[ERROR] missing required config: $name" >&2
    exit 1
  fi
}

bool_true() {
  [[ "${1:-}" == "true" || "${1:-}" == "1" || "${1:-}" == "yes" ]]
}

trim_trailing_slash() {
  local value="$1"
  echo "${value%/}"
}

ensure_agent_id() {
  if [[ -n "${AGENT_ID:-}" ]]; then
    echo "${AGENT_ID}"
    return 0
  fi

  if [[ -r "/etc/machine-id" ]]; then
    local mid
    mid="$(tr -d '\n' < /etc/machine-id)"
    if [[ -n "${mid}" ]]; then
      echo "agt_${mid:0:16}"
      return 0
    fi
  fi

  if command -v uuidgen >/dev/null 2>&1; then
    local uid
    uid="$(uuidgen | tr '[:upper:]' '[:lower:]' | tr -d '-')"
    echo "agt_${uid:0:16}"
    return 0
  fi

  echo "[ERROR] failed to derive AGENT_ID (set AGENT_ID explicitly)" >&2
  exit 1
}

detect_arch() {
  local raw
  raw="$(uname -m)"
  case "${raw}" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *)
      echo "[ERROR] unsupported architecture: ${raw}" >&2
      exit 1
      ;;
  esac
}

github_repo_path() {
  local url="$1"
  echo "${url}" | sed -E 's#^https?://github.com/##; s#\.git$##'
}

toml_string_array_from_csv() {
  local csv="$1"
  jq -rn --arg csv "${csv}" '
    ($csv
      | split(",")
      | map(gsub("^\\s+|\\s+$"; ""))
      | map(select(length > 0))) as $items
    | "[" + ($items | map(@json) | join(", ")) + "]"
  '
}

fetch_agent_authorized_keys() {
  local base_url resp
  base_url="$(trim_trailing_slash "${PLATFORM_BASE_URL}")"

  if ! resp="$(curl -fsSL -H "X-Robot-API-Key: ${REGISTERED_API_KEY}" "${base_url}/api/agent/authorized-keys")"; then
    if ! resp="$(curl -fsSL "${base_url}/api/agent-auth-public-key")"; then
      echo "[WARN] failed to fetch platform authorized keys; will keep fallback auth mode"
      return 1
    fi
  fi

  REGISTERED_AGENT_AUTH_KEYS="$({
    echo "${resp}" | jq -r '
      if (.authorized_keys | type) == "array" then
        .authorized_keys[]?
      else
        .public_key // .data.public_key // empty
      end
    ' | paste -sd, -
  } || true)"
  if [[ -z "${REGISTERED_AGENT_AUTH_KEYS}" ]]; then
    echo "[WARN] platform authorized keys missing from response; will keep fallback auth mode"
    return 1
  fi

  echo "[INFO] fetched platform authorized keys"
}

install_from_release() {
  local arch="$1"
  local repo_path
  repo_path="$(github_repo_path "${AGENT_REPO_URL}")"

  local api_url
  if [[ "${AGENT_RELEASE_TAG}" == "latest" ]]; then
    api_url="https://api.github.com/repos/${repo_path}/releases/latest"
  else
    api_url="https://api.github.com/repos/${repo_path}/releases/tags/${AGENT_RELEASE_TAG}"
  fi

  echo "[INFO] checking GitHub release assets: ${api_url}"
  local release_json
  if ! release_json="$(curl -fsSL "${api_url}")"; then
    echo "[WARN] failed to query GitHub release API"
    return 1
  fi

  local asset_name asset_url
  asset_name="$({
    echo "${release_json}" | jq -r --arg arch "${arch}" '
      .assets[]?
      | select(.name | test("robotunnel-agent"; "i"))
      | select(.name | test("linux"; "i"))
      | select(
          ($arch == "amd64" and (.name | test("amd64|x86_64"; "i"))) or
          ($arch == "arm64" and (.name | test("arm64|aarch64"; "i")))
        )
      | .name
    ' | head -n 1
  } || true)"

  asset_url="$({
    echo "${release_json}" | jq -r --arg arch "${arch}" '
      .assets[]?
      | select(.name | test("robotunnel-agent"; "i"))
      | select(.name | test("linux"; "i"))
      | select(
          ($arch == "amd64" and (.name | test("amd64|x86_64"; "i"))) or
          ($arch == "arm64" and (.name | test("arm64|aarch64"; "i")))
        )
      | .browser_download_url
    ' | head -n 1
  } || true)"

  if [[ -z "${asset_url}" || "${asset_url}" == "null" ]]; then
    echo "[WARN] no matching release binary found for linux/${arch}"
    return 1
  fi

  echo "[INFO] release asset selected: ${asset_name}"
  local tmp_dir downloaded candidate_bin
  tmp_dir="$(mktemp -d)"
  downloaded="${tmp_dir}/${asset_name}"
  curl -fL "${asset_url}" -o "${downloaded}"

  candidate_bin=""
  if [[ "${asset_name}" == *.tar.gz || "${asset_name}" == *.tgz ]]; then
    tar -xzf "${downloaded}" -C "${tmp_dir}"
    candidate_bin="$(find "${tmp_dir}" -type f -name robotunnel-agent | head -n 1 || true)"
  elif [[ "${asset_name}" == *.zip ]]; then
    require_cmd unzip
    unzip -q "${downloaded}" -d "${tmp_dir}"
    candidate_bin="$(find "${tmp_dir}" -type f -name robotunnel-agent | head -n 1 || true)"
  else
    candidate_bin="${downloaded}"
  fi

  if [[ -z "${candidate_bin}" || ! -f "${candidate_bin}" ]]; then
    echo "[WARN] downloaded asset does not contain robotunnel-agent binary"
    rm -rf "${tmp_dir}"
    return 1
  fi

  mkdir -p "${INSTALL_BIN_DIR}"
  install -m 0755 "${candidate_bin}" "${INSTALL_BIN_DIR}/robotunnel-agent"
  rm -rf "${tmp_dir}"
  echo "[INFO] installed from release -> ${INSTALL_BIN_DIR}/robotunnel-agent"
}

install_from_source() {
  require_cmd git
  require_cmd cargo

  mkdir -p "$(dirname "${AGENT_WORKDIR}")"
  if [[ ! -d "${AGENT_WORKDIR}/.git" ]]; then
    echo "[INFO] cloning agent repo into ${AGENT_WORKDIR}"
    git clone "${AGENT_REPO_URL}" "${AGENT_WORKDIR}"
  else
    echo "[INFO] updating agent repo in ${AGENT_WORKDIR}"
    git -C "${AGENT_WORKDIR}" fetch --tags --prune
  fi

  if [[ "${AGENT_RELEASE_TAG}" != "latest" ]]; then
    git -C "${AGENT_WORKDIR}" checkout "${AGENT_RELEASE_TAG}"
  else
    git -C "${AGENT_WORKDIR}" checkout main || true
    git -C "${AGENT_WORKDIR}" pull --ff-only || true
  fi

  echo "[INFO] building robotunnel-agent from source"
  (cd "${AGENT_WORKDIR}" && cargo build --release)

  mkdir -p "${INSTALL_BIN_DIR}"
  install -m 0755 "${AGENT_WORKDIR}/target/release/robotunnel-agent" "${INSTALL_BIN_DIR}/robotunnel-agent"
  echo "[INFO] built + installed -> ${INSTALL_BIN_DIR}/robotunnel-agent"
}

read_agent_config_value() {
  local key="$1"
  if [[ ! -f "${AGENT_CONFIG_PATH}" ]]; then
    return 0
  fi
  local line value
  line="$(grep -E "^[[:space:]]*${key}[[:space:]]*=" "${AGENT_CONFIG_PATH}" | head -n 1 || true)"
  if [[ -z "${line}" ]]; then
    return 0
  fi
  value="${line#*=}"
  value="$(echo "${value}" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g; s/^"//; s/"$//')"
  echo "${value}"
}

probe_api_key_on_base() {
  local base_url="$1"
  local api_key="$2"
  local resp code
  resp="$(curl -sS -X POST "${base_url}/api/heartbeat" -H 'Content-Type: application/json' -d "{\"api_key\":\"${api_key}\"}" -w $'\n%{http_code}' || true)"
  code="${resp##*$'\n'}"
  [[ "${code}" =~ ^2[0-9][0-9]$ ]]
}

try_reuse_existing_registration() {
  local preferred_base="$1"
  local existing_api_key existing_robot_id existing_api_url candidate_base
  existing_api_key="$(read_agent_config_value "api_key")"
  existing_robot_id="$(read_agent_config_value "robot_id")"
  existing_api_url="$(read_agent_config_value "api_url")"

  if [[ -z "${existing_api_key}" ]]; then
    echo "[INFO] no reusable api_key found in ${AGENT_CONFIG_PATH}" >&2
    return 1
  fi

  for candidate_base in "${preferred_base}" "${existing_api_url}"; do
    candidate_base="$(trim_trailing_slash "${candidate_base}")"
    if [[ -z "${candidate_base}" ]]; then
      continue
    fi
    if ! probe_api_key_on_base "${candidate_base}" "${existing_api_key}"; then
      continue
    fi

    REGISTERED_API_KEY="${existing_api_key}"
    REGISTERED_ROBOT_ID="${existing_robot_id}"
    REGISTERED_AGENT_ID="${AGENT_ID}"
    PLATFORM_BASE_URL="${candidate_base}"

    if [[ -z "${REGISTERED_ROBOT_ID}" ]]; then
      local ak_resp ak_body ak_code
      ak_resp="$(curl -sS -H "X-Robot-API-Key: ${existing_api_key}" "${candidate_base}/api/agent/authorized-keys" -w $'\n%{http_code}' || true)"
      ak_code="${ak_resp##*$'\n'}"
      ak_body="${ak_resp%$'\n'*}"
      if [[ "${ak_code}" =~ ^2[0-9][0-9]$ ]]; then
        REGISTERED_ROBOT_ID="$(echo "${ak_body}" | jq -r '.robot_id // .data.robot_id // empty' 2>/dev/null || true)"
      fi
    fi

    echo "[WARN] register failed, reused existing api_key from ${AGENT_CONFIG_PATH}" >&2
    echo "[INFO] continuing with existing robot_id=${REGISTERED_ROBOT_ID:-unknown} on ${candidate_base}" >&2
    return 0
  done

  echo "[INFO] existing api_key found but validation failed on available API base URLs" >&2
  return 1
}

register_robot() {
  local base_url payload resp http_code body err_msg
  base_url="$(trim_trailing_slash "${PLATFORM_BASE_URL}")"
  payload="$(jq -n \
    --arg rt_key "${RT_KEY}" \
    --arg name "${ROBOT_NAME}" \
    --arg agent_id "${AGENT_ID}" \
    --arg role "${ROBOT_ROLE}" \
    --arg avatar_url "${ROBOT_AVATAR_URL}" \
    '{rt_key: $rt_key, agent_id: $agent_id, name: $name}
     + (if $role != "" then {role: $role} else {} end)
     + (if $avatar_url != "" then {avatar_url: $avatar_url} else {} end)')"

  echo "[INFO] registering robot via ${base_url}/api/register (agent_id=${AGENT_ID})"
  if ! resp="$(curl -sS -X POST "${base_url}/api/register" -H 'Content-Type: application/json' -d "${payload}" -w $'\n%{http_code}')"; then
    echo "[ERROR] register API request failed (network or TLS error)" >&2
    exit 1
  fi

  http_code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  if [[ ! "${http_code}" =~ ^2[0-9][0-9]$ ]]; then
    err_msg="$(echo "${body}" | jq -r '.error // .message // empty' 2>/dev/null || true)"
    echo "[ERROR] register API request failed (HTTP ${http_code})" >&2
    if [[ -n "${err_msg}" ]]; then
      echo "[ERROR] ${err_msg}" >&2
    fi
    if [[ -n "${body// }" ]]; then
      echo "[ERROR] response: ${body}" >&2
    fi
    if try_reuse_existing_registration "${base_url}"; then
      return 0
    fi
    exit 1
  fi
  resp="${body}"

  REGISTERED_API_KEY="$(echo "${resp}" | jq -r '.data.api_key // .api_key // empty')"
  REGISTERED_ROBOT_ID="$(echo "${resp}" | jq -r '.data.robot_id // .robot_id // empty')"
  REGISTERED_AGENT_ID="$(echo "${resp}" | jq -r '.data.agent_id // .agent_id // empty')"

  if [[ -z "${REGISTERED_API_KEY}" ]]; then
    echo "[ERROR] register response does not contain api_key" >&2
    echo "${resp}" >&2
    exit 1
  fi

  echo "[INFO] registration done: robot_id=${REGISTERED_ROBOT_ID:-unknown} agent_id=${REGISTERED_AGENT_ID:-${AGENT_ID}}"
}

write_agent_config() {
  mkdir -p "$(dirname "${AGENT_CONFIG_PATH}")"

  local resolved_webrtc_robot_id
  resolved_webrtc_robot_id="${WEBRTC_ROBOT_ID:-${REGISTERED_ROBOT_ID:-}}"

  local merged_authorized_keys
  merged_authorized_keys="${AUTHORIZED_KEYS:-}"
  if [[ -n "${REGISTERED_AGENT_AUTH_KEYS:-}" ]]; then
    if [[ -n "${merged_authorized_keys// }" ]]; then
      merged_authorized_keys="${merged_authorized_keys},${REGISTERED_AGENT_AUTH_KEYS}"
    else
      merged_authorized_keys="${REGISTERED_AGENT_AUTH_KEYS}"
    fi
  fi

  local server_auth_block
  if [[ -n "${merged_authorized_keys// }" ]]; then
    local authorized_keys_toml
    authorized_keys_toml="$(toml_string_array_from_csv "${merged_authorized_keys}")"
    server_auth_block=$(printf 'authorized_keys = %s\ninsecure_allow_any_client = false' "${authorized_keys_toml}")
  else
    server_auth_block="insecure_allow_any_client = ${INSECURE_ALLOW_ANY_CLIENT}"
  fi

  cat > "${AGENT_CONFIG_PATH}" <<CFG
[server]
listen_port = ${AGENT_LISTEN_PORT}
${server_auth_block}

[platform]
api_url = "$(trim_trailing_slash "${PLATFORM_BASE_URL}")"
api_key = "${REGISTERED_API_KEY}"

[webrtc]
enabled = ${WEBRTC_ENABLED}
robot_id = "${resolved_webrtc_robot_id}"
stun_timeout_secs = ${WEBRTC_STUN_TIMEOUT_SECS}

[heartbeat]
interval_secs = ${HEARTBEAT_INTERVAL_SECS}

[logging]
level = "${LOG_LEVEL}"
CFG

  chmod 600 "${AGENT_CONFIG_PATH}"
  echo "[INFO] wrote agent config: ${AGENT_CONFIG_PATH}"
}

start_agent() {
  if ! bool_true "${START_AGENT}"; then
    echo "[INFO] START_AGENT=false, skip start"
    return
  fi

  if [[ -f "${AGENT_PID_PATH}" ]]; then
    local old_pid
    old_pid="$(cat "${AGENT_PID_PATH}" 2>/dev/null || true)"
    if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
      echo "[INFO] stopping old agent process pid=${old_pid}"
      kill "${old_pid}" || true
      sleep 1
    fi
  fi

  echo "[INFO] starting agent in background"
  nohup "${INSTALL_BIN_DIR}/robotunnel-agent" --config "${AGENT_CONFIG_PATH}" > "${AGENT_LOG_PATH}" 2>&1 &
  local pid=$!
  echo "${pid}" > "${AGENT_PID_PATH}"

  sleep 2
  if kill -0 "${pid}" 2>/dev/null; then
    echo "[INFO] agent started pid=${pid}"
    echo "[INFO] logs: tail -f ${AGENT_LOG_PATH}"
  else
    echo "[ERROR] agent failed to start, check log: ${AGENT_LOG_PATH}" >&2
    exit 1
  fi
}

# ---------- Defaults ----------
PLATFORM_BASE_URL="${PLATFORM_BASE_URL:-https://api.robotunnel.io}"
AGENT_REPO_URL="${AGENT_REPO_URL:-https://github.com/RussellTNY/robotunnel-agent.git}"
AGENT_INSTALL_METHOD="${AGENT_INSTALL_METHOD:-auto}"
AGENT_RELEASE_TAG="${AGENT_RELEASE_TAG:-latest}"
INSTALL_BIN_DIR="${INSTALL_BIN_DIR:-$HOME/.local/bin}"
AGENT_WORKDIR="${AGENT_WORKDIR:-$HOME/robotunnel-agent}"
AGENT_CONFIG_PATH="${AGENT_CONFIG_PATH:-$HOME/.config/robotunnel/agent.toml}"
AGENT_LISTEN_PORT="${AGENT_LISTEN_PORT:-11411}"
HEARTBEAT_INTERVAL_SECS="${HEARTBEAT_INTERVAL_SECS:-30}"
LOG_LEVEL="${LOG_LEVEL:-info}"
WEBRTC_ENABLED="${WEBRTC_ENABLED:-true}"
WEBRTC_STUN_TIMEOUT_SECS="${WEBRTC_STUN_TIMEOUT_SECS:-8}"
AUTHORIZED_KEYS="${AUTHORIZED_KEYS:-}"
INSECURE_ALLOW_ANY_CLIENT="${INSECURE_ALLOW_ANY_CLIENT:-false}"
START_AGENT="${START_AGENT:-true}"
AGENT_LOG_PATH="${AGENT_LOG_PATH:-$HOME/robotunnel-agent.log}"
AGENT_PID_PATH="${AGENT_PID_PATH:-$HOME/robotunnel-agent.pid}"
ROBOT_ROLE="${ROBOT_ROLE:-}"
ROBOT_AVATAR_URL="${ROBOT_AVATAR_URL:-}"

# ---------- Preflight ----------
require_cmd curl
require_cmd jq
require_cmd install
require_var RT_KEY

AGENT_ID="$(ensure_agent_id)"
ROBOT_NAME="${ROBOT_NAME:-$(hostname -s 2>/dev/null || echo "${AGENT_ID}")}"
ARCH="$(detect_arch)"

# ---------- Install ----------
case "${AGENT_INSTALL_METHOD}" in
  auto)
    if ! install_from_release "${ARCH}"; then
      echo "[INFO] fallback to source build"
      install_from_source
    fi
    ;;
  release)
    install_from_release "${ARCH}"
    ;;
  build)
    install_from_source
    ;;
  *)
    echo "[ERROR] unsupported AGENT_INSTALL_METHOD=${AGENT_INSTALL_METHOD}" >&2
    exit 1
    ;;
esac

# ---------- Register + Configure ----------
REGISTERED_API_KEY=""
REGISTERED_ROBOT_ID=""
REGISTERED_AGENT_ID=""
REGISTERED_AGENT_AUTH_KEYS=""

register_robot
if ! fetch_agent_authorized_keys; then
  if bool_true "${INSECURE_ALLOW_ANY_CLIENT}"; then
    echo "[WARN] continuing without authorized keys because INSECURE_ALLOW_ANY_CLIENT=true"
  elif [[ -n "${AUTHORIZED_KEYS// }" ]]; then
    echo "[WARN] platform authorized keys fetch failed; continuing with configured AUTHORIZED_KEYS"
  else
    echo "[ERROR] failed to fetch platform authorized keys and no AUTHORIZED_KEYS configured." >&2
    echo "[ERROR] Refusing to continue in secure mode. Set INSECURE_ALLOW_ANY_CLIENT=true only for explicit local development." >&2
    exit 1
  fi
fi
write_agent_config
start_agent

echo ""
echo "[DONE] Agent installation completed"
echo "  binary : ${INSTALL_BIN_DIR}/robotunnel-agent"
echo "  config : ${AGENT_CONFIG_PATH}"
echo "  robot  : ${REGISTERED_ROBOT_ID:-unknown}"
echo "  agent  : ${REGISTERED_AGENT_ID:-${AGENT_ID}}"
echo "  api_key: $(echo "${REGISTERED_API_KEY}" | sed -E 's/^(.{6}).*(.{4})$/\1...\2/')"
echo ""
echo "[NOTE] If ${INSTALL_BIN_DIR} is not in PATH, add it before using robotunnel-agent directly."
