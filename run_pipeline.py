#!/usr/bin/env python3
"""CLI (Tier 1). Examples:
  python run_pipeline.py --generate --size 100
  python run_pipeline.py --strategy all --size 100k
  python run_pipeline.py --strategy incremental
  python run_pipeline.py --benchmark --size 100k --repeat 5
"""
import argparse
import sys
from pathlib import Path

from src.config import load_config, parse_size, resolve
from src.monitoring.logger import get_logger
from src.orchestration.pipeline import ALL, Pipeline


def main(argv=None):
    ap = argparse.ArgumentParser(description="DataSync Benchmark - comparative data loading")
    ap.add_argument("--config", default=None)
    ap.add_argument("--generate", action="store_true", help="only generate the synthetic dataset")
    ap.add_argument("--size", default=None, help="orders in baseline: 100, 10k, 100k, 500k")
    ap.add_argument("--hot-days", type=int, default=None, help="concentrate changes on the most recent N days (localised changes)")
    ap.add_argument("--strategy", default="all", choices=ALL + ["all"])
    ap.add_argument("--reset", action="store_true", help="rebuild the warehouse from the baseline")
    ap.add_argument("--benchmark", action="store_true", help="repeat each strategy on fresh copies, report medians")
    ap.add_argument("--repeat", type=int, default=None)
    a = ap.parse_args(argv)

    cfg = load_config(a.config)
    log = get_logger(log_file=resolve(cfg, "log_file"))
    pipe = Pipeline(cfg, log)
    size = parse_size(a.size) if a.size else None

    if a.generate:
        if size is None:
            ap.error("--generate needs --size")
        pipe.ensure_dataset(size, a.hot_days) or log.info("Dataset already present for this size")
        return 0

    if a.benchmark:
        from src.benchmark.performance import run_benchmark
        pipe.ensure_dataset(size, a.hot_days)
        reps = a.repeat or cfg["benchmark"]["repeats"]
        df, recon = run_benchmark(cfg, reps, size_label=size)
        rep = resolve(cfg, "reports_dir"); rep.mkdir(parents=True, exist_ok=True)
        df.to_csv(rep / "benchmark_report.csv", index=False)
        import json; (rep / "benchmark_reconciliation.json").write_text(json.dumps(recon, indent=2, default=str))
        print(df.drop(columns=["size", "min_sec", "max_sec"]).to_string(index=False))
        print(recon["overall"])
        return 0 if recon["all_strategies_equivalent"] else 1

    out = pipe.run([a.strategy], reset=a.reset, size=size, hot_days=a.hot_days)
    return 0 if out["status"] in ("SUCCESS", "NO_OP") else 1


if __name__ == "__main__":
    sys.exit(main())
