from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

_PG_ENV_MAPPING = {
    "host": "PGHOST",
    "hostaddr": "PGHOSTADDR",
    "port": "PGPORT",
    "dbname": "PGDATABASE",
    "user": "PGUSER",
    "password": "PGPASSWORD",
    "passfile": "PGPASSFILE",
    "service": "PGSERVICE",
    "servicefile": "PGSERVICEFILE",
    "sslmode": "PGSSLMODE",
    "sslcert": "PGSSLCERT",
    "sslkey": "PGSSLKEY",
    "sslrootcert": "PGSSLROOTCERT",
    "sslcrl": "PGSSLCRL",
    "sslcrldir": "PGSSLCRLDIR",
    "target_session_attrs": "PGTARGETSESSIONATTRS",
    "gssencmode": "PGGSSENCMODE",
}
_FINGERPRINT_KEYS = (
    "host",
    "hostaddr",
    "port",
    "dbname",
    "user",
    "service",
    "sslmode",
    "target_session_attrs",
)


def parse_postgres_conninfo(dsn: str) -> dict[str, str]:
    try:
        from psycopg.conninfo import conninfo_to_dict
    except ImportError as exc:  # pragma: no cover - archive extra is optional
        raise RuntimeError("PostgreSQL archive operations require the 'archive' extra") from exc
    return {key: str(value) for key, value in conninfo_to_dict(dsn).items() if value is not None}


def resolved_connection_parameters(connection: object, dsn: str) -> dict[str, str]:
    parameters = parse_postgres_conninfo(dsn)
    info = getattr(connection, "info", None)
    get_parameters = getattr(info, "get_parameters", None)
    if callable(get_parameters):
        parameters.update(
            {key: str(value) for key, value in get_parameters().items() if value is not None}
        )
    for key in ("host", "hostaddr", "port", "dbname", "user"):
        value = getattr(info, key, None)
        if value is not None:
            parameters[key] = str(value)
    return parameters


def postgres_connection_fingerprint(parameters: Mapping[str, str]) -> str:
    identity = {key: str(parameters.get(key, "")) for key in _FINGERPRINT_KEYS}
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def postgres_parameters_from_environment(environment: Mapping[str, str]) -> dict[str, str]:
    return {
        parameter: str(environment[variable])
        for parameter, variable in _PG_ENV_MAPPING.items()
        if variable in environment and environment[variable] != ""
    }


def exact_pg_environment(
    parameters: Mapping[str, str], inherited: Mapping[str, str]
) -> dict[str, str]:
    """Build a libpq environment that cannot inherit a different database target."""

    environment = {key: value for key, value in inherited.items() if not key.startswith("PG")}
    for parameter, variable in _PG_ENV_MAPPING.items():
        value = parameters.get(parameter)
        if value not in (None, ""):
            environment[variable] = str(value)
    if "PGDATABASE" not in environment:
        raise ValueError("controlled PostgreSQL connection has no database name")
    return environment
