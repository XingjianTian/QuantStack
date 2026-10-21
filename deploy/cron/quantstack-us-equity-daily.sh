#!/usr/bin/env bash
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/us-equity-daily.env"
set +a

# Canonical publication must remain readable by the stable read-only consumer
# group. Keep these values in the private NAS environment instead of relying on
# the invoking user's umask (or a one-off container's root identity).
: "${QUANT_DATA_SHARED_GID:?missing QUANT_DATA_SHARED_GID}"
: "${QUANT_DATA_PERMISSION_ROOT:?missing QUANT_DATA_PERMISSION_ROOT}"
: "${QUANT_DATA_FILE_MODE:?missing QUANT_DATA_FILE_MODE}"
: "${QUANT_DATA_DIRECTORY_MODE:?missing QUANT_DATA_DIRECTORY_MODE}"

PYTHONPATH="${QUANTSTACK_US_EQUITY_REPO}/src:${QUANTSTACK_US_EQUITY_REPO}" \
  "${QUANTSTACK_US_EQUITY_PYTHON}" \
  "${QUANTSTACK_US_EQUITY_REPO}/scripts/run_us_equity_session_daily_nas.py" \
  --config "${QUANTSTACK_US_EQUITY_CONFIG}"

if [[ -n "${QUANTSTACK_US_TOTAL_RETURN_PROMOTION_CONFIG:-}" ]]; then
  PYTHONPATH="${QUANTSTACK_US_EQUITY_REPO}/src:${QUANTSTACK_US_EQUITY_REPO}" \
    "${QUANTSTACK_US_EQUITY_PYTHON}" \
    "${QUANTSTACK_US_EQUITY_REPO}/scripts/promote_us_equity_daily_total_return.py" \
    --config "${QUANTSTACK_US_TOTAL_RETURN_PROMOTION_CONFIG}"
fi
