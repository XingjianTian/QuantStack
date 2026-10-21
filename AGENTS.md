# QuantStack agent guidance

- Preserve production and NAS data. Do not delete, overwrite, or broaden Provider requests without explicit authorization.
- Run repository Python entrypoints with both source roots available: `PYTHONPATH=<repo>/src:<repo>`.
- For Docker commands that execute a script from an immutable QuantStack checkout, always set both `--workdir <repo>` and `-e PYTHONPATH=<repo>/src:<repo>`. Do not launch a bare `docker run ... python /absolute/path/to/script.py`; importing the script by path does not make its checkout modules importable.
- Prefer the checked-in cron entrypoints for routine operations. When a one-off container is genuinely required, copy the canonical command from `docs/US_EQUITY_FULL_MARKET_OPERATIONS.md` and retain its logs until acceptance completes.
- Before starting a recovery container, confirm that no daily or promotion process is already running. Never use a recovery launch to add Provider retries or parallel collection.
