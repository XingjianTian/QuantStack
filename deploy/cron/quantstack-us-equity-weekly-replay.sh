#!/usr/bin/env bash
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/us-equity-daily.env"
set +a

PYTHONPATH="${QUANTSTACK_US_EQUITY_REPO}/src:${QUANTSTACK_US_EQUITY_REPO}" \
  "${QUANTSTACK_US_EQUITY_PYTHON}" \
  "${QUANTSTACK_US_EQUITY_REPO}/scripts/run_session_parquet_weekly_replay.py" \
  --root "${QUANTSTACK_US_SESSION_PARQUET_ROOT}" \
  --market US
