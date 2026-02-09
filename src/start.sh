#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOTFS_DIR="${SCRIPT_DIR}/rootfs"
ROOTFS_BIN_DIR="${ROOTFS_DIR}/bin/fs-daemon"
ROOTFS_ETC_DIR="${ROOTFS_DIR}/etc"

OPT_BASE_DIR="/opt/buckyos"
OPT_BIN_DIR="${OPT_BASE_DIR}/bin/fs-daemon"
OPT_BIN_PATH="${OPT_BIN_DIR}/fs_daemon"
OPT_ETC_DIR="${OPT_BASE_DIR}/etc"
LOG_DIR="${OPT_BASE_DIR}/var/log/fs-daemon"
LOG_FILE="${LOG_DIR}/fs_daemon.log"

MOUNTPOINT="/opt/ndm"
REQUIRED_CONFIGS=("store_layout.json" "fs_daemon.json")

echo "[1/7] cargo build -p fs_daemon"
cd "${SCRIPT_DIR}"
cargo build -p fs_daemon

TARGET_DIR="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' | head -n 1)"
if [ -z "${TARGET_DIR}" ]; then
  TARGET_DIR="${CARGO_TARGET_DIR:-${SCRIPT_DIR}/target}"
  if [[ "${TARGET_DIR}" != /* ]]; then
    TARGET_DIR="${SCRIPT_DIR}/${TARGET_DIR}"
  fi
fi

BUILD_BIN_PATH="${TARGET_DIR}/debug/fs_daemon"
if [ ! -x "${BUILD_BIN_PATH}" ]; then
  echo "build output missing: ${BUILD_BIN_PATH}" >&2
  exit 1
fi

echo "[2/7] copy build binary to rootfs: ${ROOTFS_BIN_DIR}/fs_daemon"
mkdir -p "${ROOTFS_BIN_DIR}"
install -m 0755 "${BUILD_BIN_PATH}" "${ROOTFS_BIN_DIR}/fs_daemon"

echo "[3/7] copy rootfs binary to ${OPT_BIN_PATH}"
mkdir -p "${OPT_BIN_DIR}"
install -m 0755 "${ROOTFS_BIN_DIR}/fs_daemon" "${OPT_BIN_PATH}"

echo "[4/7] copy rootfs/etc/*.json to ${OPT_ETC_DIR} (no overwrite)"
mkdir -p "${ROOTFS_ETC_DIR}" "${OPT_ETC_DIR}"
shopt -s nullglob
json_files=("${ROOTFS_ETC_DIR}"/*.json)
shopt -u nullglob

if [ "${#json_files[@]}" -eq 0 ]; then
  echo "no json found under ${ROOTFS_ETC_DIR}" >&2
  echo "please prepare at least: store_layout.json, fs_daemon.json" >&2
  exit 1
fi

for src_json in "${json_files[@]}"; do
  file_name="$(basename "${src_json}")"
  dst_json="${OPT_ETC_DIR}/${file_name}"
  if [ -e "${dst_json}" ]; then
    echo "skip existing: ${dst_json}"
    continue
  fi
  cp "${src_json}" "${dst_json}"
  echo "copied: ${src_json} -> ${dst_json}"
done

missing_configs=()
for config_name in "${REQUIRED_CONFIGS[@]}"; do
  if [ ! -f "${OPT_ETC_DIR}/${config_name}" ]; then
    missing_configs+=("${OPT_ETC_DIR}/${config_name}")
  fi
done
if [ "${#missing_configs[@]}" -gt 0 ]; then
  echo "missing required default config(s):" >&2
  for missing in "${missing_configs[@]}"; do
    echo "  ${missing}" >&2
  done
  exit 1
fi

echo "[5/7] ensure mountpoint exists: ${MOUNTPOINT}"
mkdir -p "${MOUNTPOINT}"

echo "[6/7] start fs_daemon with nohup and default config paths"
mkdir -p "${LOG_DIR}"
nohup "${OPT_BIN_PATH}" "${MOUNTPOINT}" >"${LOG_FILE}" 2>&1 &
DAEMON_PID=$!

echo "[7/7] show startup result"
sleep 2
if ! kill -0 "${DAEMON_PID}" 2>/dev/null; then
  echo "fs_daemon exited early, check log: ${LOG_FILE}" >&2
  tail -n 80 "${LOG_FILE}" >&2 || true
  exit 1
fi

echo "fs_daemon started."
echo "pid       : ${DAEMON_PID}"
echo "mountpoint: ${MOUNTPOINT}"
echo "binary    : ${OPT_BIN_PATH}"
echo "log       : ${LOG_FILE}"
ps -p "${DAEMON_PID}" -o pid=,ppid=,stat=,command=
mount | grep -E " on ${MOUNTPOINT} " || true
tail -n 20 "${LOG_FILE}" || true
