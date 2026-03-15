#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OPS_SCRIPT="${SCRIPT_DIR}/fuse_ops_verify.sh"

if [ ! -x "${OPS_SCRIPT}" ]; then
  echo "missing executable ops script: ${OPS_SCRIPT}" >&2
  exit 1
fi

RUN_ROOT="${FS_DAEMON_E2E_ROOT:-$(mktemp -d /tmp/fs_daemon_e2e.XXXXXX)}"
STORE_CFG="${RUN_ROOT}/store_layout.json"
SERVICE_CFG="${RUN_ROOT}/fs_daemon.json"
MOUNTPOINT="${RUN_ROOT}/mnt"
LOG_FILE="${RUN_ROOT}/fs_daemon.log"
SESSION_FILE="${RUN_ROOT}/session.env"

mkdir -p "${RUN_ROOT}/stores/store-a" "${RUN_ROOT}/stores/store-b" "${RUN_ROOT}/stores/store-c"
mkdir -p "${MOUNTPOINT}"
MOUNTPOINT_REAL="$(cd "${MOUNTPOINT}" && pwd -P)"

cat > "${STORE_CFG}" <<JSON
{
  "epoch": 1,
  "stores": [
    {"store_id": "store-a", "path": "${RUN_ROOT}/stores/store-a", "weight": 1},
    {"store_id": "store-b", "path": "${RUN_ROOT}/stores/store-b", "weight": 1},
    {"store_id": "store-c", "path": "${RUN_ROOT}/stores/store-c", "weight": 1}
  ]
}
JSON

cat > "${SERVICE_CFG}" <<JSON
{
  "instance_id": "fs-daemon-e2e",
  "fs_buffer_dir": "${RUN_ROOT}/fs_buffer",
  "fs_meta_db_path": "${RUN_ROOT}/fs_meta/fs_meta.db",
  "fs_buffer_size_limit": 0
}
JSON

is_mounted() {
  if command -v mountpoint >/dev/null 2>&1; then
    if mountpoint -q "${MOUNTPOINT}" || mountpoint -q "${MOUNTPOINT_REAL}"; then
      return 0
    fi
  fi
  mount | grep -E " on (${MOUNTPOINT}|${MOUNTPOINT_REAL}) " >/dev/null 2>&1
}

is_mount_ready() {
  is_mounted && ls "${MOUNTPOINT}" >/dev/null 2>&1
}

cd "${WORKSPACE_ROOT}"

echo "starting fs_daemon..."
echo "  workspace : ${WORKSPACE_ROOT}"
echo "  run_root  : ${RUN_ROOT}"
echo "  mountpoint: ${MOUNTPOINT}"

echo "building fs_daemon binary..."
cargo build -p fs_daemon >"${LOG_FILE}" 2>&1

TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' | head -n 1)"
if [ -z "${TARGET_DIR}" ]; then
  TARGET_DIR="${CARGO_TARGET_DIR:-target}"
  if [[ "${TARGET_DIR}" != /* ]]; then
    TARGET_DIR="${WORKSPACE_ROOT}/${TARGET_DIR}"
  fi
fi
DAEMON_BIN="${FS_DAEMON_BIN:-${TARGET_DIR}/debug/fs_daemon}"
if [ ! -x "${DAEMON_BIN}" ]; then
  echo "fs_daemon binary not found: ${DAEMON_BIN}" >&2
  exit 1
fi

echo "daemon bin: ${DAEMON_BIN}"
nohup "${DAEMON_BIN}" "${MOUNTPOINT}" --store-config "${STORE_CFG}" --service-config "${SERVICE_CFG}" >>"${LOG_FILE}" 2>&1 &
DAEMON_PID=$!

echo "daemon pid: ${DAEMON_PID}"
echo "daemon log: ${LOG_FILE}"

for _ in $(seq 1 60); do
  if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
    echo "fs_daemon exited before mount was ready" >&2
    tail -n 120 "${LOG_FILE}" >&2 || true
    exit 1
  fi
  if is_mount_ready; then
    break
  fi
  sleep 1
done

if ! is_mount_ready; then
  echo "mount not ready after timeout: ${MOUNTPOINT}" >&2
  echo "mountpoint(real): ${MOUNTPOINT_REAL}" >&2
  mount | grep -E "fuse|macfuse|${MOUNTPOINT_REAL}|${MOUNTPOINT}" >&2 || true
  tail -n 120 "${LOG_FILE}" >&2 || true
  exit 1
fi

echo "mount ready: ${MOUNTPOINT}"
"${OPS_SCRIPT}" "${MOUNTPOINT}"

if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
  echo "fs_daemon exited after verification; expected to remain running" >&2
  tail -n 120 "${LOG_FILE}" >&2 || true
  exit 1
fi

cat > "${SESSION_FILE}" <<ENV
FS_DAEMON_E2E_ROOT=${RUN_ROOT}
FS_DAEMON_PID=${DAEMON_PID}
FS_DAEMON_MOUNTPOINT=${MOUNTPOINT}
FS_DAEMON_LOG=${LOG_FILE}
FS_DAEMON_STORE_CONFIG=${STORE_CFG}
FS_DAEMON_SERVICE_CONFIG=${SERVICE_CFG}
ENV

echo
echo "E2E verification passed. fs_daemon remains running for manual inspection."
echo "session file : ${SESSION_FILE}"
echo "mountpoint   : ${MOUNTPOINT}"
echo "daemon pid   : ${DAEMON_PID}"
echo
echo "manual check examples:"
echo "  ls -la ${MOUNTPOINT}"
echo "  cat ${LOG_FILE}"
echo
echo "when done, stop manually:"
echo "  kill ${DAEMON_PID}"
echo "  # Linux: fusermount -u ${MOUNTPOINT}"
echo "  # macOS: umount ${MOUNTPOINT}"
