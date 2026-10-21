from __future__ import annotations

import hashlib
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

_ARCHIVE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]+$")
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_SENSITIVE_TABLE_PARTS = (
    "appsetting",
    "auth",
    "credential",
    "oauth",
    "password",
    "secret",
    "session",
    "token",
    "user",
)
_WRITE_SQL = re.compile(
    r"\b(ALTER|ANALYZE|CLUSTER|COMMENT|CREATE|DELETE|DROP|GRANT|INSERT|LOCK|"
    r"REINDEX|REVOKE|TRUNCATE|UPDATE|VACUUM)\b",
    re.IGNORECASE,
)
_SECRET_PATTERNS = {
    "credential_assignment": re.compile(
        r"(?i)\b(password|passwd|token|api[_-]?key|secret|client[_-]?secret)\b"
        r"\s*[:=]\s*['\"]?[^\s'\",}]{6,}"
    ),
    "database_uri_with_credentials": re.compile(
        r"(?i)\b(postgres(?:ql)?|mysql|mongodb(?:\+srv)?)://[^\s/@:]+:[^\s/@]+@"
    ),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "email_identifier": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I),
    "private_ipv4": re.compile(
        r"\b(?:10\.(?:\d{1,3}\.){2}\d{1,3}|192\.168\.(?:\d{1,3}\.)\d{1,3}|"
        r"172\.(?:1[6-9]|2\d|3[01])\.(?:\d{1,3}\.)\d{1,3})\b"
    ),
    "private_hostname": re.compile(
        r"(?i)\b[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.(?:internal|lan|local)\b"
    ),
    "private_path": re.compile(r"(?:/Users/[^/\s]+|/home/[^/\s]+|/Volumes/[^/\s]+)"),
}


def validate_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"unsafe PostgreSQL identifier: {value!r}")
    return value


def assert_read_only_sql(sql: str) -> None:
    stripped = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S)
    if _WRITE_SQL.search(stripped):
        raise ValueError("SQL violates the read-only archive boundary")
    statements = [item.strip() for item in stripped.split(";") if item.strip()]
    allowed = (
        "SELECT",
        "WITH",
        "SHOW",
        "SET LOCAL",
        "SET TRANSACTION",
        "BEGIN",
        "ROLLBACK",
        "COMMIT",
    )
    for statement in statements:
        if not statement.upper().startswith(allowed):
            raise ValueError("SQL is not an approved read-only statement")


def is_secret_bearing_table(table_name: str) -> bool:
    compact = re.sub(r"[^a-z0-9]", "", table_name.lower())
    return any(part in compact for part in _SENSITIVE_TABLE_PARTS)


def allowed_backup_tables(
    inventory_tables: list[str] | tuple[str, ...],
    *,
    required: tuple[str, ...] = ("stock_prices", "stock_universe"),
) -> tuple[str, ...]:
    available = set(inventory_tables)
    missing = set(required) - available
    if missing:
        raise ValueError(f"missing required legacy table(s): {', '.join(sorted(missing))}")
    return tuple(name for name in required if not is_secret_bearing_table(name))


def validate_backup_listing(
    listing: str,
    *,
    required_tables: tuple[str, ...] = ("stock_prices", "stock_universe"),
) -> None:
    table_data: set[str] = set()
    for line in listing.splitlines():
        marker = " TABLE DATA "
        if marker not in line:
            continue
        fields = line.split(marker, 1)[1].split()
        if len(fields) >= 2:
            table_data.add(fields[1])
    missing = set(required_tables) - table_data
    if missing:
        raise ValueError(f"backup listing is missing table data: {', '.join(sorted(missing))}")
    sensitive = sorted(name for name in table_data if is_secret_bearing_table(name))
    if sensitive:
        raise ValueError("backup listing contains secret-bearing table data")


def validate_schema_listing(
    listing: str,
    *,
    required_tables: tuple[str, ...] = ("stock_prices", "stock_universe"),
) -> tuple[int, int]:
    lines = tuple(
        line for line in listing.splitlines() if line.strip() and not line.startswith(";")
    )
    table_data_count = sum(" TABLE DATA " in line for line in lines)
    if table_data_count:
        raise ValueError("schema snapshot unexpectedly contains table data")
    missing = {
        table
        for table in required_tables
        if not any(
            f" TABLE public {table} " in f"{line} "
            or f" TABLE ATTACH public {table} " in f"{line} "
            for line in lines
        )
    }
    if missing:
        raise ValueError(
            f"schema snapshot is missing table definitions: {', '.join(sorted(missing))}"
        )
    return len(lines), table_data_count


def scan_secret_text(text: str, *, schema_only: bool = False) -> tuple[str, ...]:
    # Schema-only SQL may contain sensitive *column names*, which do not match
    # the value-oriented expressions. Defaults and embedded values remain scanned.
    patterns = _SECRET_PATTERNS
    return tuple(name for name, pattern in patterns.items() if pattern.search(text))


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_archive_id(source: str, snapshot_time: datetime, nonce: str | None = None) -> str:
    if snapshot_time.tzinfo is None:
        raise ValueError("snapshot_time must be timezone-aware")
    resolved_nonce = nonce or secrets.token_hex(8)
    timestamp = snapshot_time.strftime("%Y%m%dT%H%M%SZ")
    identity = hashlib.sha256(
        f"{source}|{snapshot_time.isoformat()}|{resolved_nonce}".encode()
    ).hexdigest()[:12]
    return f"ss-legacy-{timestamp.lower()}-{identity}"


def assert_restore_target_isolated(source_fingerprint: str, target_fingerprint: str) -> None:
    if not source_fingerprint or not target_fingerprint:
        raise ValueError("source and restore target fingerprints are required")
    if secrets.compare_digest(source_fingerprint, target_fingerprint):
        raise ValueError("restore target resolves to the source database")


@dataclass(frozen=True)
class ArchiveLayout:
    root: Path
    archive_id: str

    def __post_init__(self) -> None:
        if not _ARCHIVE_ID.fullmatch(self.archive_id):
            raise ValueError("invalid archive ID")
        object.__setattr__(self, "root", self.root.expanduser().resolve())

    def resolve(self, relative: str | Path) -> Path:
        value = Path(relative)
        if value.is_absolute():
            raise ValueError("archive paths must be relative")
        resolved = (self.root / value).resolve()
        if resolved != self.root and self.root not in resolved.parents:
            raise ValueError("archive path traversal rejected")
        return resolved

    def archive_path(self, section: str, *parts: str) -> Path:
        validate_identifier(section.replace("-", "_"))
        return self.resolve(Path(section) / self.archive_id / Path(*parts))

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        for section in ("backups", "frozen-exports", "verification", "reports", "state"):
            path = self.archive_path(section)
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(path, 0o700)

    def logical_uri(self, section: str, *parts: str) -> str:
        suffix = Path(section) / self.archive_id / Path(*parts)
        return f"archive://ss-legacy/{suffix.as_posix()}"
