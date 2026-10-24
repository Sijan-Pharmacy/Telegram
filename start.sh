#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
mkdir -p logs data
if [[ -f .skygen.pid ]] && kill -0 "$(cat .skygen.pid)" 2>/dev/null; then
  echo "SkyGen is already running (PID $(cat .skygen.pid))." >&2
  exit 1
fi
echo $$ > .skygen.pid
cleanup() { rm -f .skygen.pid; }
trap cleanup EXIT
exec python -u lifecycle.py
