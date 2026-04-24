#!/bin/bash
set -euo pipefail

OPTIONS_PATH="/data/options.json"

read_option() {
  python - "$OPTIONS_PATH" "$1" <<'PY'
import json
import sys
from pathlib import Path

options_path = Path(sys.argv[1])
key = sys.argv[2]
try:
    payload = json.loads(options_path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
value = payload.get(key, "")
print(value if value is not None else "")
PY
}

WORKSPACE_PATH="$(read_option workspace_path)"
CONFIG_PATH="$(read_option config_path)"
POLL_SECONDS="$(read_option poll_seconds)"
TIMEZONE_NAME="$(read_option timezone)"
CODEX_HOME_VALUE="$(read_option codex_home)"
GITHUB_TOKEN="$(read_option github_token)"
GITHUB_REPO="$(read_option github_repo)"
GITHUB_BRANCH="$(read_option github_branch)"
GITHUB_RESEARCH_PATH="$(read_option github_research_path)"

if [ -z "${WORKSPACE_PATH}" ]; then
  WORKSPACE_PATH="/share/polymarket_EDEC_bot"
fi
if [ -z "${CONFIG_PATH}" ]; then
  CONFIG_PATH="/share/edec/config/active_config.yaml"
fi
if [ "${CONFIG_PATH}" = "edec_bot/config_phase_a_single.yaml" ] || [ "${CONFIG_PATH}" = "config_phase_a_single.yaml" ]; then
  CONFIG_PATH="/share/edec/config/active_config.yaml"
fi
if [ -z "${POLL_SECONDS}" ]; then
  POLL_SECONDS="15"
fi
if [ -z "${TIMEZONE_NAME}" ]; then
  TIMEZONE_NAME="America/Edmonton"
fi
if [ -z "${CODEX_HOME_VALUE}" ]; then
  CODEX_HOME_VALUE="/data/codex"
fi
if [ -z "${GITHUB_BRANCH}" ]; then
  GITHUB_BRANCH="main"
fi
if [ -z "${GITHUB_RESEARCH_PATH}" ]; then
  GITHUB_RESEARCH_PATH="research_exports"
fi

if [ ! -d "${WORKSPACE_PATH}" ]; then
  echo "Workspace path not found: ${WORKSPACE_PATH}" >&2
  exit 1
fi
if [ ! -f "${WORKSPACE_PATH}/edec_bot/requirements.txt" ]; then
  echo "Missing repo requirements at ${WORKSPACE_PATH}/edec_bot/requirements.txt" >&2
  exit 1
fi

mkdir -p /share/edec
mkdir -p "${CODEX_HOME_VALUE}"
VENV_DIR="${CODEX_HOME_VALUE}/venv"
REQ_HASH_FILE="${CODEX_HOME_VALUE}/requirements.sha256"
REQ_FILE="${WORKSPACE_PATH}/edec_bot/requirements.txt"

export PYTHONPATH="${WORKSPACE_PATH}"
export EDEC_SHARED_DATA_ROOT="/share/edec"
export EDEC_LOCAL_TIMEZONE="${TIMEZONE_NAME}"
export CODEX_HOME="${CODEX_HOME_VALUE}"
export EDEC_CONFIG_PATH="${CONFIG_PATH}"
export EDEC_GITHUB_TOKEN="${GITHUB_TOKEN}"
export EDEC_GITHUB_REPO="${GITHUB_REPO}"
export EDEC_GITHUB_BRANCH="${GITHUB_BRANCH}"
export EDEC_GITHUB_RESEARCH_PATH="${GITHUB_RESEARCH_PATH}"

CURRENT_REQ_HASH="$(python - "${REQ_FILE}" <<'PY'
import hashlib
import sys
from pathlib import Path

path = Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
PY
)"
SAVED_REQ_HASH=""
if [ -f "${REQ_HASH_FILE}" ]; then
  SAVED_REQ_HASH="$(cat "${REQ_HASH_FILE}")"
fi

if [ ! -x "${VENV_DIR}/bin/python" ]; then
  python -m venv "${VENV_DIR}"
fi

if [ "${CURRENT_REQ_HASH}" != "${SAVED_REQ_HASH}" ]; then
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/python" -m pip install --no-cache-dir -r "${REQ_FILE}"
  printf '%s' "${CURRENT_REQ_HASH}" > "${REQ_HASH_FILE}"
fi

cd "${WORKSPACE_PATH}"
exec "${VENV_DIR}/bin/python" -m edec_bot.research codex-runner --poll-seconds "${POLL_SECONDS}"
