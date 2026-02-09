#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <mountpoint>" >&2
  exit 2
fi

MOUNTPOINT="$1"
CASE_DIR="${MOUNTPOINT}/e2e_case"
FILE_A="${CASE_DIR}/alpha.txt"
FILE_B="${CASE_DIR}/renamed.txt"
FILE_C="${CASE_DIR}/nested/blob.bin"

mkdir -p "${CASE_DIR}/nested"

PAYLOAD="fs-daemon-e2e-$(date +%s)"
printf '%s\n' "${PAYLOAD}" > "${FILE_A}"

READ_BACK="$(cat "${FILE_A}")"
if [ "${READ_BACK}" != "${PAYLOAD}" ]; then
  echo "payload mismatch: expected='${PAYLOAD}' got='${READ_BACK}'" >&2
  exit 1
fi

mv "${FILE_A}" "${FILE_B}"
if [ -e "${FILE_A}" ]; then
  echo "rename failed: source still exists ${FILE_A}" >&2
  exit 1
fi

if [ "$(cat "${FILE_B}")" != "${PAYLOAD}" ]; then
  echo "rename failed: target content mismatch" >&2
  exit 1
fi

# Validate larger write/read path.
dd if=/dev/zero of="${FILE_C}" bs=1024 count=8 status=none
SIZE_BYTES="$(wc -c < "${FILE_C}" | tr -d ' ')"
if [ "${SIZE_BYTES}" != "8192" ]; then
  echo "size mismatch for ${FILE_C}: ${SIZE_BYTES}" >&2
  exit 1
fi

ls -la "${CASE_DIR}" >/dev/null
ls -la "${CASE_DIR}/nested" >/dev/null

# macOS finder-like copy path: host dir -> mounted dir via ditto + xattrs.
if [ "$(uname -s)" = "Darwin" ] && command -v ditto >/dev/null 2>&1 && command -v xattr >/dev/null 2>&1; then
  HOST_SRC="$(mktemp -d /tmp/fs_daemon_finder_src.XXXXXX)"
  FINDER_DST="${MOUNTPOINT}/finder_copy_case"
  mkdir -p "${HOST_SRC}"
  cp /bin/ls "${HOST_SRC}/ls.bin"
  printf 'finder-copy-%s\n' "$(date +%s)" > "${HOST_SRC}/a.txt"
  xattr -w com.apple.FinderInfo "0123456789ABCDEF0123456789ABCDEF" "${HOST_SRC}/a.txt"
  xattr -w com.apple.quarantine "0081;65f2f3b5;Safari;" "${HOST_SRC}/a.txt"

  if ! ditto "${HOST_SRC}" "${FINDER_DST}" >/tmp/fs_daemon_ditto.out 2>/tmp/fs_daemon_ditto.err; then
    echo "ditto copy to mount failed" >&2
    cat /tmp/fs_daemon_ditto.err >&2 || true
    exit 1
  fi

  test -f "${FINDER_DST}/ls.bin"
  test -f "${FINDER_DST}/a.txt"
  rm -rf "${HOST_SRC}" "${FINDER_DST}" /tmp/fs_daemon_ditto.out /tmp/fs_daemon_ditto.err
fi

rm -f "${FILE_B}" "${FILE_C}"
rmdir "${CASE_DIR}/nested"

echo "fuse_ops_verify: success"
