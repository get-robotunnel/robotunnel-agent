#!/usr/bin/env bash
set -euo pipefail

VNC_PORT=5901
DISPLAY_ID="${RT_RVIZ_DISPLAY:-:99}"
SCREEN_GEOMETRY="${RT_RVIZ_SCREEN:-1280x800x24}"
RVIZ_CONFIG="${RT_RVIZ_CONFIG:-}"
RVIZ_LOG="${RT_RVIZ_LOG:-/tmp/rt_rviz2.log}"
VNC_LOG="${RT_RVIZ_VNC_LOG:-/tmp/rt_x11vnc.log}"
LOCALHOST_ONLY="${RT_RVIZ_VNC_LOCALHOST_ONLY:-1}"
ALLOW_NO_PASSWORD="${RT_RVIZ_VNC_ALLOW_NO_PASSWORD:-}"
VNC_PASSWORD="${RT_RVIZ_VNC_PASSWORD:-}"
VNC_PASSWORD_FILE="${RT_RVIZ_VNC_PASSWORD_FILE:-}"

is_truthy() {
  case "${1,,}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vnc-port)
      VNC_PORT="${2:-}"
      shift 2
      ;;
    --display)
      DISPLAY_ID="${2:-}"
      shift 2
      ;;
    --rviz-config)
      RVIZ_CONFIG="${2:-}"
      shift 2
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

for dep in bash Xvfb x11vnc rviz2; do
  if ! command -v "$dep" >/dev/null 2>&1; then
    echo "missing dependency: $dep" >&2
    exit 127
  fi
done

XVFB_PID=""
RVIZ_PID=""
X11VNC_PID=""
AUTH_TMP_FILE=""

cleanup() {
  if [[ -n "$X11VNC_PID" ]]; then
    kill "$X11VNC_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$RVIZ_PID" ]]; then
    kill "$RVIZ_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$XVFB_PID" ]]; then
    kill "$XVFB_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$AUTH_TMP_FILE" ]]; then
    rm -f "$AUTH_TMP_FILE" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

Xvfb "$DISPLAY_ID" -screen 0 "$SCREEN_GEOMETRY" -ac +extension GLX +render -noreset >/dev/null 2>&1 &
XVFB_PID="$!"
sleep 0.4

export DISPLAY="$DISPLAY_ID"
if [[ -n "$RVIZ_CONFIG" && -f "$RVIZ_CONFIG" ]]; then
  rviz2 -d "$RVIZ_CONFIG" >>"$RVIZ_LOG" 2>&1 &
else
  rviz2 >>"$RVIZ_LOG" 2>&1 &
fi
RVIZ_PID="$!"
sleep 0.8

if [[ -z "$ALLOW_NO_PASSWORD" ]]; then
  if is_truthy "$LOCALHOST_ONLY"; then
    ALLOW_NO_PASSWORD=1
  else
    ALLOW_NO_PASSWORD=0
  fi
fi

X11VNC_ARGS=(
  -display "$DISPLAY_ID"
  -rfbport "$VNC_PORT"
  -forever
  -shared
  -quiet
)

if is_truthy "$LOCALHOST_ONLY"; then
  X11VNC_ARGS+=(-localhost)
fi

if is_truthy "$ALLOW_NO_PASSWORD"; then
  X11VNC_ARGS+=(-nopw)
else
  if [[ -n "$VNC_PASSWORD_FILE" ]]; then
    if [[ ! -f "$VNC_PASSWORD_FILE" ]]; then
      echo "RT_RVIZ_VNC_PASSWORD_FILE does not exist: $VNC_PASSWORD_FILE" >&2
      exit 2
    fi
    X11VNC_ARGS+=(-rfbauth "$VNC_PASSWORD_FILE")
  elif [[ -n "$VNC_PASSWORD" ]]; then
    AUTH_TMP_FILE="$(mktemp /tmp/rt-vnc-passwd.XXXXXX)"
    x11vnc -storepasswd "$VNC_PASSWORD" "$AUTH_TMP_FILE" >/dev/null 2>&1
    X11VNC_ARGS+=(-rfbauth "$AUTH_TMP_FILE")
  else
    echo "x11vnc auth required: set RT_RVIZ_VNC_PASSWORD_FILE or RT_RVIZ_VNC_PASSWORD, or explicitly allow RT_RVIZ_VNC_ALLOW_NO_PASSWORD=1" >&2
    exit 2
  fi
fi

x11vnc "${X11VNC_ARGS[@]}" >>"$VNC_LOG" 2>&1 &
X11VNC_PID="$!"

while true; do
  if [[ -n "$X11VNC_PID" ]] && ! kill -0 "$X11VNC_PID" >/dev/null 2>&1; then
    wait "$X11VNC_PID"
    exit $?
  fi
  if [[ -n "$RVIZ_PID" ]] && ! kill -0 "$RVIZ_PID" >/dev/null 2>&1; then
    wait "$RVIZ_PID"
    exit $?
  fi
  if [[ -n "$XVFB_PID" ]] && ! kill -0 "$XVFB_PID" >/dev/null 2>&1; then
    wait "$XVFB_PID"
    exit $?
  fi
  sleep 1
done
