import json
import logging

from src.benchmark.performance import run_benchmark
from src.config import load_config
from src.orchestration.pipeline import Pipeline


def pipe(tmp_path):
    cfg = load_config(root=tmp_path)
    return cfg, Pipeline(cfg, logging.getLogger("test"))


def test_full_pipeline_all_strategies_reconcile_and_write_reports(tmp_path):
    cfg, p = pipe(tmp_path)
    out = p.run(["all"], size=400)
    assert out["status"] == "SUCCESS"
    assert all(o["status"] == "SUCCESS" for o in out["results"].values())
    assert out["reconciliation"]["all_strategies_equivalent"]
    rep = tmp_path / "data" / "reports"
    assert json.loads((rep / "reconciliation_report.json").read_text())["overall"] == "ALL STRATEGIES EQUIVALENT"
    dq = json.loads((rep / "data_quality_report.json").read_text())
    assert dq["rejected_records"] > 0 and dq["input_records"] == dq["valid_records"] + dq["rejected_records"]
    assert (rep / "performance_report.csv").exists()
    assert list((tmp_path / "data" / "quarantine").glob("rejected_records.*"))


def test_rerun_is_idempotent(tmp_path):
    cfg, p = pipe(tmp_path)
    p.run(["all"], size=300)
    again = p.run(["all"])
    assert all(o["status"] == "SKIPPED_DUPLICATE_BATCH" for o in again["results"].values())
    assert again["reconciliation"]["all_strategies_equivalent"]


def test_missing_column_fails_batch_and_loads_nothing(tmp_path):
    cfg, p = pipe(tmp_path)
    p.run(["all"], size=200)
    ev = tmp_path / "data" / "raw" / "incoming" / "order_events.csv"
    ev.write_text(ev.read_text().replace("operation", "op", 1))
    out = p.run(["all"])
    assert out["status"] == "FAILED" and "missing required column" in out["error"]


def test_missing_source_file_fails_cleanly(tmp_path):
    cfg, p = pipe(tmp_path)
    p.run(["all"], size=200)
    (tmp_path / "data" / "raw" / "incoming" / "order_events.csv").unlink()
    assert p.run(["all"])["status"] == "FAILED"


def test_empty_batch_is_a_controlled_noop(tmp_path):
    cfg, p = pipe(tmp_path)
    p.run(["all"], size=200)
    ev = tmp_path / "data" / "raw" / "incoming" / "order_events.csv"
    ev.write_text(ev.read_text().splitlines()[0] + "\n")
    assert p.run(["all"])["status"] == "NO_OP"


def test_benchmark_reports_medians_and_reconciles(tmp_path):
    cfg, p = pipe(tmp_path)
    p.ensure_dataset(500)
    df, recon = run_benchmark(cfg, repeats=3, size_label=500)
    assert list(df["strategy"]) == ["full_refresh", "incremental", "partition_replace"]
    assert (df["median_sec"] > 0).all() and (df["reconciliation"] == "PASS").all()
    assert recon["all_strategies_equivalent"]
    assert df["final_row_count"].nunique() == 1
