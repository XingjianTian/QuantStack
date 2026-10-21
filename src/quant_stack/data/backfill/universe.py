from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date

from .models import (
    BackfillUniverse,
    BackfillUniverseInstrument,
    BackfillUniverseSource,
)


def _identity(code: str) -> tuple[str, str, str] | None:
    if len(code) != 9 or code[2] != "." or not code[3:].isdigit():
        return None
    if code.startswith("sh.") and code[3:6] in {"600", "601", "603", "605", "688"}:
        symbol = code[3:]
        return f"XSHG.{symbol}", symbol, "XSHG"
    if code.startswith("sz.") and code[3:6] in {
        "000",
        "001",
        "002",
        "003",
        "300",
        "301",
    }:
        symbol = code[3:]
        return f"XSHE.{symbol}", symbol, "XSHE"
    return None


def _board(venue: str, symbol: str) -> str:
    if venue == "XSHG":
        return "star" if symbol.startswith("688") else "main"
    if symbol.startswith(("300", "301")):
        return "chinext"
    if symbol.startswith("002"):
        return "sme"
    return "main"


def _era(first_observed: date) -> str:
    if first_observed.year <= 2015:
        return "observed_2015_or_earlier"
    if first_observed.year <= 2019:
        return "first_observed_2016_2019"
    if first_observed.year <= 2022:
        return "first_observed_2020_2022"
    return "first_observed_2023_plus"


def _score(seed: int, bucket: str, instrument_id: str) -> str:
    return hashlib.sha256(f"{seed}|{bucket}|{instrument_id}".encode()).hexdigest()


def _stratified_select(
    candidates: Sequence[BackfillUniverseInstrument], *, seed: int, count: int
) -> list[BackfillUniverseInstrument]:
    buckets: dict[str, list[BackfillUniverseInstrument]] = defaultdict(list)
    for item in candidates:
        buckets[item.selection_bucket].append(item)
    for bucket, items in buckets.items():
        items.sort(key=lambda item: _score(seed, bucket, item.instrument_id))
    selected: list[BackfillUniverseInstrument] = []
    ordered_buckets = sorted(buckets)
    while len(selected) < count:
        progressed = False
        for bucket in ordered_buckets:
            if buckets[bucket]:
                selected.append(buckets[bucket].pop(0))
                progressed = True
                if len(selected) == count:
                    break
        if not progressed:
            raise ValueError("historical snapshot union is too small for requested universe")
    return selected


def _observed_candidates(
    snapshots: Sequence[tuple[date, Sequence[Mapping[str, str]]]],
    sources: Sequence[BackfillUniverseSource],
) -> tuple[list[date], dict[str, list[BackfillUniverseInstrument]]]:
    if len(snapshots) != len(sources):
        raise ValueError("each snapshot requires exactly one source reference")
    observed_dates = [item[0] for item in snapshots]
    if observed_dates != sorted(set(observed_dates)):
        raise ValueError("snapshot dates must be unique and ascending")
    if observed_dates[0].year > 2015 or observed_dates[-1].year < 2025:
        raise ValueError("historical snapshots must cover 2015 through a recent year")

    observations: dict[str, list[date]] = defaultdict(list)
    identities: dict[str, tuple[str, str, str]] = {}
    for observed_date, rows in snapshots:
        seen: set[str] = set()
        for row in rows:
            if set(row) != {"code", "tradeStatus", "code_name"}:
                raise ValueError("unexpected BaoStock query_all_stock native schema")
            identity = _identity(str(row["code"]))
            if identity is None:
                continue
            instrument_id, _symbol, _venue = identity
            if instrument_id in seen:
                raise ValueError("duplicate instrument in provider snapshot")
            seen.add(instrument_id)
            identities[instrument_id] = identity
            observations[instrument_id].append(observed_date)

    latest = observed_dates[-1]
    candidates_by_venue: dict[str, list[BackfillUniverseInstrument]] = defaultdict(list)
    for instrument_id, dates in observations.items():
        _, symbol, venue = identities[instrument_id]
        membership = "current_observed" if dates[-1] == latest else "historical_only_observed"
        bucket = f"{_board(venue, symbol)}:{_era(dates[0])}:{membership}"
        candidates_by_venue[venue].append(
            BackfillUniverseInstrument(
                instrument_id=instrument_id,
                symbol=symbol,
                venue=venue,
                provider_symbol=("sh." if venue == "XSHG" else "sz.") + symbol,
                selection_bucket=bucket,
                boundary_evidence="unavailable",
                first_observed_membership_date=dates[0],
                last_observed_membership_date=dates[-1],
                membership_evidence="provider_observed",
            )
        )
    return observed_dates, candidates_by_venue


def build_historical_universe(
    snapshots: Sequence[tuple[date, Sequence[Mapping[str, str]]]],
    sources: Sequence[BackfillUniverseSource],
    *,
    universe_id: str,
    version: str,
    selection_seed: int,
    target_count: int,
) -> BackfillUniverse:
    """Build a deterministic, exchange-balanced universe from provider observations."""

    if not 300 <= target_count <= 500 or target_count % 2:
        raise ValueError("medium universe target_count must be an even value from 300 to 500")
    _observed_dates, candidates_by_venue = _observed_candidates(snapshots, sources)

    per_venue = target_count // 2
    selected = []
    for venue in ("XSHG", "XSHE"):
        selected.extend(
            _stratified_select(
                candidates_by_venue[venue],
                seed=selection_seed,
                count=per_venue,
            )
        )
    selected.sort(key=lambda item: ({"XSHG": 0, "XSHE": 1}[item.venue], item.symbol))
    universe = BackfillUniverse(
        universe_id=universe_id,
        version=version,
        selection_seed=selection_seed,
        selection_policy=(
            f"Fixed-hash round-robin selection of {per_venue} instruments per exchange, "
            "stratified by provider-observed era, board, and recent-vs-historical membership."
        ),
        source_policy=(
            "Union of BaoStock query_all_stock observations at versioned historical dates. "
            "Membership dates are provider observations only, never inferred listing or "
            "delisting facts; BJSE and non-equity code families are excluded."
        ),
        snapshot_versions=tuple(item.snapshot_version for item in sources),
        source_refs=tuple(sources),
        instruments=tuple(selected),
    )
    return universe.model_copy(update={"declared_content_sha256": universe.content_sha256})


def build_full_historical_universe(
    snapshots: Sequence[tuple[date, Sequence[Mapping[str, str]]]],
    sources: Sequence[BackfillUniverseSource],
    *,
    universe_id: str,
    version: str,
) -> BackfillUniverse:
    """Keep every XSHG/XSHE A-share observed in the versioned snapshot set."""

    _observed_dates, candidates_by_venue = _observed_candidates(snapshots, sources)
    if not candidates_by_venue["XSHG"] or not candidates_by_venue["XSHE"]:
        raise ValueError("full historical universe requires both XSHG and XSHE observations")
    selected = sorted(
        (*candidates_by_venue["XSHG"], *candidates_by_venue["XSHE"]),
        key=lambda item: ({"XSHG": 0, "XSHE": 1}[item.venue], item.symbol),
    )
    universe = BackfillUniverse(
        universe_id=universe_id,
        version=version,
        selection_seed=0,
        selection_policy=(
            "Complete deterministic union of every XSHG/XSHE A-share identity observed "
            "in the versioned BaoStock historical snapshots; no sampling."
        ),
        source_policy=(
            "Union of BaoStock query_all_stock observations at versioned historical dates. "
            "Membership dates are provider observations only, never inferred listing or "
            "delisting facts; BJSE and non-equity code families are excluded."
        ),
        snapshot_versions=tuple(item.snapshot_version for item in sources),
        source_refs=tuple(sources),
        instruments=tuple(selected),
    )
    return universe.model_copy(update={"declared_content_sha256": universe.content_sha256})
