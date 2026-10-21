"""Loopback-only workbench for executing the fictional brokerage sample.

This app is deliberately separate from the production API. Its fixed fixtures,
account identity and business clock cannot be changed through HTTP requests.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import ipaddress
import json
import os
import threading
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from quant_stack import brokerage_cli as cli
from quant_stack.investing.budgets import load_ledger, order_occupation

SAMPLE_TIME = "2026-10-16T00:00:00+00:00"
STALE_TIME = "2026-10-16T01:00:00+00:00"
DEFAULT_RUNTIME = Path.home() / ".local" / "state" / "quantstack-brokerage-workbench"
_ALLOWED_ORIGINS = {
    "http://127.0.0.1:5187",
    "http://localhost:5187",
    "http://127.0.0.1:8917",
    "http://localhost:8917",
}
# CLI printing is process-global. All workbench reads and writes use this lock.
_OPERATION_LOCK = threading.RLock()
ZERO = Decimal("0")


class SampleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    profile: Literal["ready", "blocked"]


def _loopback(value: str) -> bool:
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _capture(handler, args):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        handler(args)
    return json.loads(output.getvalue())


def _operations(path: Path) -> dict:
    if not path.exists():
        return {
            "sync_count": 0,
            "propose_count": 0,
            "last_sync_at": None,
            "last_propose_at": None,
        }
    if path.is_symlink():
        raise ValueError("invalid sample operations record")
    return json.loads(path.read_text(encoding="utf-8"))


def _record_operation(path: Path, operation: str) -> None:
    values = _operations(path)
    values[f"{operation}_count"] += 1
    values[f"last_{operation}_at"] = datetime.now(UTC).isoformat()
    temporary = path.with_suffix(".tmp")
    # O_NOFOLLOW prevents an unrelated local symlink from redirecting the write.
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(values, stream)
    os.replace(temporary, path)


def _funding(snapshot, proposal):
    if snapshot is None:
        return None
    occupation = order_occupation(snapshot)
    balance = next((item for item in snapshot.account.balances if item.currency == "USD"), None)
    available = balance.available_cash if balance else None
    included = balance.order_reservation_included if balance else None
    reasons = list(occupation.blocking_reasons)
    if available is None:
        reasons.append("AVAILABLE_CASH_UNKNOWN")
    if included is None:
        reasons.append("ORDER_RESERVATION_SEMANTICS_UNKNOWN")
    usable = None
    if available is not None and included is not None and not occupation.blocking_reasons:
        usable = max(available - (ZERO if included else occupation.amount), ZERO)
    items = proposal["items"] if proposal else []
    cash_use = sum((Decimal(item["maximum_cash_use"]) for item in items), ZERO)
    fees = sum((Decimal(item["estimated_fee"]) for item in items), ZERO)
    reserve = sum((Decimal(item["cash_reserve"]) for item in items), ZERO)
    return {
        "currency": "USD",
        "available_cash": str(available) if available is not None else None,
        "buying_power": str(balance.buying_power)
        if balance and balance.buying_power is not None
        else None,
        "open_order_occupation": str(occupation.amount),
        "reservation_included": included,
        "cash_after_open_orders": str(usable) if usable is not None else None,
        "proposal_cash_use": str(cash_use),
        "proposal_fees": str(fees),
        "cash_reserve": str(reserve),
        "remaining_cash_after_proposals": str(max(usable - cash_use, ZERO))
        if usable is not None
        else None,
        "blocking_reasons": list(dict.fromkeys(reasons)),
        "order_ids": list(occupation.order_ids),
        "uncertain_order_ids": list(occupation.uncertain_order_ids),
    }


def _calculations(proposal):
    if proposal is None:
        return []
    result = []
    for item in proposal["items"]:
        # Preserve generator outputs; do not reimplement its quantity decisions.
        before = Decimal(item["strategy_budget"]) - Decimal(item["spent_budget"])
        result.append(
            {
                **item,
                "budget_before": str(max(before, ZERO)),
                "budget_after_proposal": item["remaining_budget"],
                "formula": (
                    "整股数量取预算、可用现金与共享持仓上限允许数量的最小值；"
                    "预算和现金均先预留费用与现金余量，再除以假设限价向下取整。"
                    "已有碎股、期权接股义务及本批次先前建议共同占用持仓上限。"
                    "剩余预算扣除拟买金额与费用，建议本身不扣实际账本。"
                ),
            }
        )
    return result


def create_app(runtime_root: Path | None = None) -> FastAPI:
    application = FastAPI(
        title="QuantStack fictional local account workbench", docs_url=None, redoc_url=None
    )
    root = (
        runtime_root
        if runtime_root is not None
        else Path(os.environ.get("QUANT_SAMPLE_RUNTIME_ROOT", str(DEFAULT_RUNTIME)))
    )
    site_origin = os.environ.get("QUANT_SAMPLE_SITE_ORIGIN", "").strip()
    allowed_origins = _ALLOWED_ORIGINS | ({site_origin} if site_origin else set())

    @application.middleware("http")
    async def local_only(request: Request, call_next):
        host = urlsplit("http://" + request.headers.get("host", "")).hostname or ""
        if not _loopback(host) or request.client is None or not _loopback(request.client.host):
            return JSONResponse({"detail": "Local sample access only"}, status_code=403)
        origin = request.headers.get("origin")
        if origin is not None and origin not in allowed_origins:
            return JSONResponse({"detail": "Origin is not allowed"}, status_code=403)
        if request.query_params and (
            request.method != "GET" or set(request.query_params) - {"profile", "stale"}
        ):
            return JSONResponse({"detail": "Unsupported sample parameter"}, status_code=422)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    def args_for(profile: str, stale: bool = False):
        return argparse.Namespace(
            profile=profile,
            fixture_root=str(cli.DEFAULT_FIXTURE_ROOT),
            state_root=str(root / profile),
            account_id=cli.DEFAULT_ACCOUNT_ID,
            account_hash=cli.DEFAULT_ACCOUNT_HASH,
            at=STALE_TIME if stale else SAMPLE_TIME,
        )

    def view(profile: str, stale: bool = False):
        args = args_for(profile, stale)
        store = cli._store(args)
        snapshot, _ = cli._current_sample_view(store, args.account_id)
        state = _capture(cli.command_state, args)
        proposal = state["current_proposal"]
        ledger = load_ledger(store, args.account_id)
        return {
            "snapshot": snapshot.model_dump(mode="json") if snapshot else None,
            "state": state,
            "proposal": proposal,
            "ledger": ledger.model_dump(mode="json") if ledger else None,
            "plan": cli._sample_plan(args.account_id).model_dump(mode="json"),
            "operations": _operations(store.root / "operations.json"),
            "funding": _funding(snapshot, proposal),
            "calculations": _calculations(proposal),
            "profile": profile,
            "fictional": True,
            "non_executable": True,
        }

    @application.get("/v1/brokerage-sample/state")
    def get_state(profile: Literal["ready", "blocked"] = "ready", stale: bool = False):
        with _OPERATION_LOCK:
            try:
                return view(profile, stale)
            except Exception as exc:
                raise HTTPException(500, "Unable to read local fictional sample") from exc

    def mutate(body: SampleRequest, operation: str):
        with _OPERATION_LOCK:
            try:
                args = args_for(body.profile)
                store = cli._store(args)
                if operation == "propose":
                    # The CLI can auto-sync, but the workbench makes both actions explicit.
                    if cli._current_sample_view(store, args.account_id)[0] is None:
                        raise HTTPException(409, "Synchronize the fictional account first")
                handler = cli.command_sync if operation == "sync" else cli.command_propose
                _capture(handler, args)
                _record_operation(store.root / "operations.json", operation)
                return view(body.profile)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(500, "Local fictional sample operation failed") from exc

    @application.post("/v1/brokerage-sample/sync")
    def synchronize(body: SampleRequest):
        return mutate(body, "sync")

    @application.post("/v1/brokerage-sample/propose")
    def propose(body: SampleRequest):
        return mutate(body, "propose")

    return application


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8917)


if __name__ == "__main__":
    main()
