#!/usr/bin/env bash
# Historical Phase 2.14 Core 500 entrypoint. Not part of the production cron set.
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/us-core-daily.env"
set +a

PYTHONPATH="${QUANTSTACK_US_CORE_REPO}/src:${QUANTSTACK_US_CORE_REPO}" \
  "${QUANTSTACK_US_CORE_PYTHON}" \
  "${QUANTSTACK_US_CORE_REPO}/scripts/run_us_equity_core_daily_nas.py" \
  daily \
  --config "${QUANTSTACK_US_CORE_CONFIG}"
