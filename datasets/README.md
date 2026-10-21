# Dataset metadata

Only catalog manifests, schemas, quality rules, and tiny test fixtures belong in
Git. Payloads resolve through `quant-data://` against `QUANT_DATA_ROOT` and live
on the NAS data lake.
