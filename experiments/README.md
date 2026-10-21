# Experiment bundles

The CLI creates one immutable directory per run. Each bundle contains copied
strategy and dataset manifests, parameters, code/upstream commits, raw result
JSON, and an automatically initialized Markdown report. Large generated bundles
belong on the NAS and are ignored by Git; selected small golden baselines may be
added under `tests/fixtures/` after review.
