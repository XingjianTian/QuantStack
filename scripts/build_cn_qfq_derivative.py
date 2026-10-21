#!/usr/bin/env python3
"""Build one immutable CN qfq derivative from raw bars and factor evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_stack.data.qfq_derivative import build_qfq_derivative


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-bars", type=Path, required=True)
    parser.add_argument("--factors", type=Path, required=True)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inputs = {args.raw_bars.resolve(), args.factors.resolve()}
    if args.corpus is not None:
        inputs.add(args.corpus.resolve())
    if args.output.resolve() in inputs:
        raise ValueError("qfq output must not overlap an input")
    report = build_qfq_derivative(
        raw_bars_path=args.raw_bars,
        factor_path=args.factors,
        corpus_path=args.corpus,
        output_path=args.output,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
