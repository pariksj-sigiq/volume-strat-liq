#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="${1:-}"
APP_DIR="${APP_DIR:-/opt/liq-sweep}"
SERVICE_NAME="${SERVICE_NAME:-liq-sweep}"
LOCK_FILE="${LOCK_FILE:-/tmp/liq-sweep-deploy.lock}"
DB_PATH="${APP_DIR}/data/nse_data.db"
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
VENV_DIR="${VENV_DIR:-${APP_DIR}/.venv}"

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

mkdir -p "${APP_DIR}" "${APP_DIR}/data"

(
  flock -n 9 || {
    echo "Another liq-sweep deploy is already running." >&2
    exit 4
  }

  echo "Data layer is external to deploys; preserving ${APP_DIR}/data"
  echo "Syncing application code into ${APP_DIR}"
  rsync -a --delete \
    --exclude ".git/" \
    --exclude ".github/" \
    --include ".env.example" \
    --exclude ".env" \
    --exclude ".env.*" \
    --exclude ".venv/" \
    --exclude "data/" \
    --exclude "reports/" \
    --exclude "artifacts/" \
    --exclude "__pycache__/" \
    --exclude "*.pyc" \
    --exclude ".pytest_cache/" \
    "${SOURCE_DIR}/" "${APP_DIR}/"

  test -s "${DB_PATH}"
  chown -R ec2-user:ec2-user "${APP_DIR}"

  echo "Installing Python runtime dependencies into ${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/python" -m pip install -e "${APP_DIR}"

  echo "Ensuring ${SERVICE_NAME} runs with the managed virtualenv"
  mkdir -p "/etc/systemd/system/${SERVICE_NAME}.service.d"
  cat >"/etc/systemd/system/${SERVICE_NAME}.service.d/override.conf" <<EOF
[Service]
WorkingDirectory=${APP_DIR}
Environment=PYTHONPATH=${APP_DIR}
EnvironmentFile=-${APP_DIR}/.env
ExecStart=
ExecStart=${VENV_DIR}/bin/python -m app.server --host 127.0.0.1 --port 8877 --db-path ${DB_PATH}
EOF
  systemctl daemon-reload

  echo "Restarting ${SERVICE_NAME}"
  systemctl restart "${SERVICE_NAME}"

  echo "Checking service and HTTP health"
  for attempt in {1..20}; do
    if systemctl is-active --quiet "${SERVICE_NAME}" \
      && curl -fsS --max-time 20 http://127.0.0.1:8877/api/health >/dev/null \
      && curl -fsS --max-time 90 http://127.0.0.1:8877/api/meta >/dev/null; then
      break
    fi
    if [[ "${attempt}" == "20" ]]; then
      systemctl --no-pager --full status "${SERVICE_NAME}" || true
      journalctl -u "${SERVICE_NAME}" -n 80 --no-pager || true
      exit 7
    fi
    sleep 2
  done

  echo "Deploy complete."
) 9>"${LOCK_FILE}"
