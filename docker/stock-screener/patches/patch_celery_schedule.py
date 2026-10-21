"""Apply the QuantStack Celery scheduling policy to stock-screener v1.4.0."""

import sys
from pathlib import Path


def _replace_exactly_once(source: str, anchor: str, replacement: str, label: str) -> str:
    matches = source.count(anchor)
    if matches != 1:
        raise ValueError(f"expected exactly one {label} anchor, found {matches}")
    return source.replace(anchor, replacement, 1)


def patch_source(source: str) -> str:
    source = _replace_exactly_once(
        source,
        "    _enabled_markets = list(SUPPORTED_MARKETS)\n",
        "    _enabled_markets = settings.enabled_markets_list\n",
        "enabled-markets",
    )

    shared_marker = "    # --- Market-agnostic / shared beat entries below ---\n"
    market_policy = """    # QuantStack owns the CN daily price lifecycle. Keep the upstream
    # market-jobs lane available for explicit maintenance, but do not enqueue
    # work for a worker that is intentionally profiled out.
    _market_jobs_enabled = os.getenv(
        'SS_MARKET_JOBS_ENABLED', 'true'
    ).strip().lower() in {'1', 'true', 'yes', 'on'}
    if not _market_jobs_enabled:
        beat_schedule = {
            name: entry
            for name, entry in beat_schedule.items()
            if not name.startswith((
                'daily-market-pipeline-',
                'weekly-ibd-classification-sync-',
            ))
        }

"""
    source = _replace_exactly_once(
        source,
        shared_marker,
        market_policy + shared_marker,
        "shared-schedule",
    )

    merge_marker = "    # Merge shared entries into the fanned-out schedule and install.\n"
    feature_policy = """    if not settings.feature_themes:
        for _theme_entry in (
            'daily-theme-candidate-promotion',
            'daily-theme-lifecycle-policies',
            'poll-content-sources',
            'hourly-reprocess-failed-themes',
            'periodic-theme-extraction',
            'periodic-theme-metrics',
        ):
            _shared_entries.pop(_theme_entry, None)

"""
    source = _replace_exactly_once(
        source,
        merge_marker,
        feature_policy + merge_marker,
        "shared-schedule-merge",
    )
    if source.count("settings.enabled_markets_list") != 1:
        raise ValueError("enabled-markets postcondition failed")
    if source.count("SS_MARKET_JOBS_ENABLED") != 1:
        raise ValueError("market-jobs policy postcondition failed")
    if source.count("if not settings.feature_themes:") != 1:
        raise ValueError("theme policy postcondition failed")
    return source


def main() -> None:
    path = Path(sys.argv[1])
    path.write_text(patch_source(path.read_text(encoding="utf-8")), encoding="utf-8")


if __name__ == "__main__":
    main()
