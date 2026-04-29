#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/liq-sweep}"
BACKUP_DIR="${BACKUP_DIR:-/opt/liq-sweep-backups}"
DB_PATH="${APP_DIR}/data/nse_data.db"

if [[ ! -f "${DB_PATH}" ]]; then
  echo "Database not found at ${DB_PATH}." >&2
  exit 3
fi

mkdir -p "${BACKUP_DIR}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="${BACKUP_DIR}/nse_data-${timestamp}.db"

echo "Backing up ${DB_PATH} to ${backup_path}"
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "${DB_PATH}" "PRAGMA wal_checkpoint(FULL);"
  sqlite3 "${DB_PATH}" ".backup '${backup_path}'"
else
  cp -p "${DB_PATH}" "${backup_path}"
fi

test -s "${backup_path}"
gzip -f "${backup_path}"
chown -R ec2-user:ec2-user "${BACKUP_DIR}"

echo "Backup complete: ${backup_path}.gz"
