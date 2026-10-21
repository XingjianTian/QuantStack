#!/usr/bin/env bash
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/daily-worker.env"
set +a

PYTHONPATH="${QUANTSTACK_DAILY_REPO}/src:${QUANTSTACK_DAILY_REPO}" \
  "${QUANTSTACK_DAILY_PYTHON}" \
  "${QUANTSTACK_DAILY_REPO}/scripts/run_session_parquet_weekly_replay.py" \
  --root "${QUANTSTACK_CN_SESSION_PARQUET_ROOT}" \
  --market CN
