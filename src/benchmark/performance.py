"""Fair benchmark: identical inputs, identical initial state (fresh copy of the same baseline DB
file for every repetition), median of N runs. Timed region = strategy.execute + COMMIT."""
import shutil
import statistics
import tempfile
from pathlib import Path

import pandas as pd

from src.orchestration.pipeline import expected_state, prepare_batch, run_strategy
from src.reconciliation.comparator import reconcile
from src.storage import repositories as repo
from src.storage.database import TARGETS, connect, init_db, read_table, transaction
from src.strategies import get_strategy

ORDER = ["full_refresh", "incremental", "partition_replace"]


def run_benchmark(cfg, repeats=5, baseline_dir=None, incoming_dir=None, size_label=None):
    batch = prepare_batch(cfg, baseline_dir, incoming_dir)
    tmp = Path(tempfile.mkdtemp(prefix="dsb_"))
    try:
        snap = tmp / "snapshot.db"
        c = connect(snap)
        init_db(c)
        with transaction(c):
            repo.load_baseline(c, batch.customers, batch.baseline)
        c.close()
        rows, finals, expected = [], {}, None
        for name in ORDER:
            times, last = [], None
            for i in range(repeats):
                db = tmp / f"{name}_{i}.db"
                shutil.copy(snap, db)
                conn = connect(db)
                out = run_strategy(conn, get_strategy(name), batch, f"bench_{name}_{i}")
                if out["status"] != "SUCCESS":
                    raise RuntimeError(f"{name} failed during benchmark: {out['error']}")
                times.append(out["duration"])
                last = out
                if i == repeats - 1:
                    finals[name] = read_table(conn, TARGETS[name])
                    expected = expected if expected is not None else expected_state(conn)
                conn.close()
                size_mb = db.stat().st_size / 1e6
                if i < repeats - 1:
                    db.unlink()
            r, med = last["result"], statistics.median(times)
            rows.append({
                "strategy": name, "size": size_label, "repeats": repeats,
                "input_records": batch.input_records, "valid_records": len(batch.valid_events),
                "rejected_records": len(batch.rejected), "inserted": r.inserted, "updated": r.updated,
                "deleted": r.deleted, "skipped_stale": r.skipped_stale,
                "final_row_count": r.final_row_count,
                "median_sec": round(med, 4), "min_sec": round(min(times), 4), "max_sec": round(max(times), 4),
                "records_per_sec": round(len(batch.valid_events) / med) if med else None,
                "rows_written": r.extra.get("rows_written", r.extra.get("rows_rewritten", r.inserted + r.updated + r.deleted)),
                "partitions_affected": r.extra.get("partitions_affected"),
                "db_size_mb": round(size_mb, 2)})
        recon = reconcile(finals, expected)
        for row in rows:
            row["reconciliation"] = recon["strategies"][row["strategy"]]["status"]
        return pd.DataFrame(rows), recon
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
