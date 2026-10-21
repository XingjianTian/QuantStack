#!/usr/bin/env bash
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/cn-operations-audit.env"
set +a
export PYTHONPATH="${QUANTSTACK_CN_AUDIT_REPO}/src:${QUANTSTACK_CN_AUDIT_REPO}"

"${QUANTSTACK_CN_AUDIT_PYTHON}" \
  "${QUANTSTACK_CN_AUDIT_REPO}/scripts/capture_cn_operations_runtime.py" \
  --config "${QUANTSTACK_CN_RUNTIME_CONFIG}" \
  --output "${QUANTSTACK_CN_RUNTIME_EVIDENCE}"
"${QUANTSTACK_CN_AUDIT_PYTHON}" \
  "${QUANTSTACK_CN_AUDIT_REPO}/scripts/audit_cn_operations.py" \
  --work-root "${QUANTSTACK_CN_WORK_ROOT}" \
  --scheduler-config "${QUANTSTACK_DAILY_CONFIG}" \
  --promotion-state "${QUANTSTACK_CN_PROMOTION_STATE}" \
  --promotion-work-root "${QUANTSTACK_CN_PROMOTION_WORK_ROOT}" \
  --runtime-evidence "${QUANTSTACK_CN_RUNTIME_EVIDENCE}" \
  --output "${QUANTSTACK_CN_AUDIT_REPORT}" \
  --history-root "${QUANTSTACK_CN_AUDIT_HISTORY_ROOT}"
