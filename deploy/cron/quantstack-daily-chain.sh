#!/usr/bin/env bash
set -euo pipefail

set -a
source "${HOME}/.config/quantstack/daily-worker.env"
set +a
PYTHONPATH="${QUANTSTACK_DAILY_REPO}/src:${QUANTSTACK_DAILY_REPO}" \
  "${QUANTSTACK_DAILY_PYTHON}" \
  "${QUANTSTACK_DAILY_REPO}/scripts/run_daily_incremental.py" \
  --config "${QUANTSTACK_DAILY_CONFIG}"

set -a
source "${HOME}/.config/quantstack/cn-qfq-promotion.env"
set +a
if [[ -n "${QUANTSTACK_QFQ_DOCKER_IMAGE:-}" ]]; then
  : "${QUANTSTACK_QFQ_DATA_ROOT:?set QUANTSTACK_QFQ_DATA_ROOT for container publication}"
  : "${QUANTSTACK_QFQ_REPO:?set QUANTSTACK_QFQ_REPO for container publication}"
  : "${QUANT_DATA_SHARED_GID:?set QUANT_DATA_SHARED_GID for container publication}"
  : "${QUANT_DATA_PERMISSION_ROOT:?set QUANT_DATA_PERMISSION_ROOT for container publication}"
  docker run --rm \
    --name quantstack-cn-qfq-daily \
    --network host \
    --user 0:0 \
    --workdir "${QUANTSTACK_QFQ_REPO}" \
    -e "PYTHONPATH=${QUANTSTACK_QFQ_REPO}/src:${QUANTSTACK_QFQ_REPO}" \
    -e "QUANT_DATA_SHARED_GID=${QUANT_DATA_SHARED_GID}" \
    -e "QUANT_DATA_PERMISSION_ROOT=${QUANT_DATA_PERMISSION_ROOT}" \
    -e "QUANT_DATA_FILE_MODE=${QUANT_DATA_FILE_MODE:-0640}" \
    -e "QUANT_DATA_DIRECTORY_MODE=${QUANT_DATA_DIRECTORY_MODE:-2750}" \
    -v "${QUANTSTACK_QFQ_DATA_ROOT}:${QUANTSTACK_QFQ_DATA_ROOT}" \
    -v "${QUANTSTACK_QFQ_REPO}:${QUANTSTACK_QFQ_REPO}:ro" \
    -v "${QUANTSTACK_QFQ_CONFIG}:${QUANTSTACK_QFQ_CONFIG}:ro" \
    "${QUANTSTACK_QFQ_DOCKER_IMAGE}" \
    python "${QUANTSTACK_QFQ_REPO}/scripts/run_cn_qfq_session_daily.py" \
    --config "${QUANTSTACK_QFQ_CONFIG}" \
    --state "${QUANTSTACK_QFQ_STATE}"
else
  PYTHONPATH="${QUANTSTACK_QFQ_REPO}/src:${QUANTSTACK_QFQ_REPO}" \
    "${QUANTSTACK_QFQ_PYTHON}" \
    "${QUANTSTACK_QFQ_REPO}/scripts/run_cn_qfq_session_daily.py" \
    --config "${QUANTSTACK_QFQ_CONFIG}" \
    --state "${QUANTSTACK_QFQ_STATE}"
fi
