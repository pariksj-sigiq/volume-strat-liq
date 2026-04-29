#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${1:-}"
APP_DIR="${APP_DIR:-/opt/liq-sweep}"
BACKUP_DIR="${BACKUP_DIR:-/opt/liq-sweep-backups}"
SERVICE_NAME="${SERVICE_NAME:-liq-sweep}"
LOCK_FILE="${LOCK_FILE:-/tmp/liq-sweep-deploy.lock}"
DB_PATH="${APP_DIR}/data/nse_data.db"

if [[ -z "${SOURCE_DIR}" || ! -d "${SOURCE_DIR}" ]]; then
  echo "Source directory is required." >&2
  exit 2
fi

if [[ "${SOURCE_DIR}" == "${APP_DIR}" ]]; then
  echo "Source and app directories must be different." >&2
  exit 2
fi

if [[ ! -f "${DB_PATH}" ]]; then
  echo "Refusing to deploy: existing database not found at ${DB_PATH}." >&2
  exit 3
fi

if [[ -f "${SOURCE_DIR}/data/nse_data.db" ]]; then
  echo "Refusing to deploy: repository checkout contains data/nse_data.db." >&2
  exit 3
fi

mkdir -p "${APP_DIR}" "${APP_DIR}/data" "${BACKUP_DIR}"

(
  flock -n 9 || {
    echo "Another liq-sweep deploy is already running." >&2
    exit 4
  }

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

  echo "Syncing application code into ${APP_DIR}"
  rsync -a --delete \
    --exclude ".git/" \
    --exclude ".github/" \
    --include ".env.example" \
    --exclude ".env" \
    --exclude ".env.*" \
    --exclude "data/" \
    --exclude "reports/" \
    --exclude "artifacts/" \
    --exclude "__pycache__/" \
    --exclude "*.pyc" \
    --exclude ".pytest_cache/" \
    "${SOURCE_DIR}/" "${APP_DIR}/"

  test -s "${DB_PATH}"
  chown -R ec2-user:ec2-user "${APP_DIR}" "${BACKUP_DIR}"

  echo "Restarting ${SERVICE_NAME}"
  systemctl restart "${SERVICE_NAME}"

  echo "Checking service and HTTP health"
  systemctl is-active --quiet "${SERVICE_NAME}"
  curl -fsS --max-time 20 http://127.0.0.1:8877/api/health >/dev/null
  curl -fsS --max-time 90 http://127.0.0.1:8877/api/meta >/dev/null

  echo "Pruning old backups, keeping the newest 10"
  find "${BACKUP_DIR}" -maxdepth 1 -name "nse_data-*.db.gz" -type f -printf "%T@ %p\n" \
    | sort -nr \
    | awk 'NR > 10 {print $2}' \
    | xargs -r rm -f

  echo "Deploy complete."
) 9>"${LOCK_FILE}"
