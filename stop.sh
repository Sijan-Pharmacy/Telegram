#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
PID_FILE=".skygen.pid"
if [[ ! -f "$PID_FILE" ]]; then
  echo "No .skygen.pid found. If this is GitHub Actions, cancel the workflow from the Actions page."
  exit 0
fi
PID="$(cat "$PID_FILE")"
if ! [[ "$PID" =~ ^[0-9]+$ ]]; then
  echo "Invalid PID file; refusing to signal a process." >&2
  exit 1
fi
if kill -0 "$PID" 2>/dev/null; then
  echo "Requesting graceful SkyGen shutdown (PID $PID); runtime will try to back up SQLite."
  kill -TERM "$PID"
  for _ in $(seq 1 60); do
    kill -0 "$PID" 2>/dev/null || { rm -f "$PID_FILE"; echo "SkyGen stopped."; exit 0; }
    sleep 1
  done
  echo "SkyGen has not exited after 60 seconds. Check logs before force-stopping it." >&2
  exit 1
else
  rm -f "$PID_FILE"
  echo "Stale PID file removed; SkyGen was not running."
fi
