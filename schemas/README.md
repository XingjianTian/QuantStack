# Schemas

Schemas are versioned contracts. Backward-compatible additions increment the
minor version; breaking changes require a new major schema and a migration note.
Runtime Pydantic models mirror these JSON Schemas and are covered by tests.

Data Layer contracts live under `schemas/data/`. They use a shared CN/HK/US
market vocabulary and are validated both as JSON Schema and as runtime models.
