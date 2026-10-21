"""Provider-neutral market data foundation for QuantStack."""

from .pipeline import SyntheticPipelineResult, run_synthetic_pipeline

__all__ = ["SyntheticPipelineResult", "run_synthetic_pipeline"]
