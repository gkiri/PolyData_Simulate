#!/usr/bin/env bash
#
# Start the Polymarket production collector in the background and keep it
# running after you disconnect SSH. Safe to re-run; it refuses to start if an
# instance is already running. Logs go to logs/collector.out.
#
# Usage:
#   chmod +x run_collector.sh
#   ./run_collector.sh
#
# Stop:
#   ./stop_collector.sh   (see companion script)
#
set -euo pipefail

ROOT="/home/Polymarket_Datacollector"
SCRIPT="unified_collector_production.py"
PID_FILE="${ROOT}/collector.pid"
LOCK_FILE="${ROOT}/data/.collector.lock"
LOG_DIR="${ROOT}/logs"
LOG_FILE="${LOG_DIR}/collector.out"

cd "${ROOT}"

# Prevent duplicate instances
if pgrep -f "${SCRIPT}" >/dev/null 2>&1; then
  echo "Collector already running. PID(s):"
  pgrep -f "${SCRIPT}"
  exit 0
fi

# If a stale lock exists, remove it (only if no process is running)
if [ -f "${LOCK_FILE}" ]; then
  echo "Removing stale lock: ${LOCK_FILE}"
  rm -f "${LOCK_FILE}"
fi

mkdir -p "${LOG_DIR}"

# Start in background, detached from terminal
nohup setsid python3 "${SCRIPT}" > "${LOG_FILE}" 2>&1 < /dev/null &
PID=$!
echo "${PID}" > "${PID_FILE}"

echo "Collector started. PID=${PID}"
echo "Logs: ${LOG_FILE}"

