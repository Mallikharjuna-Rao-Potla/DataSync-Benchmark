import tempfile
from pathlib import Path

import pandas as pd

from src.ingestion import data_generator
from src.orchestration.pipeline import prepare_batch
from src.config import load_config
from src.storage import repositories as repo
from src.storage.database import ORDER_COLS, TARGETS, connect, init_db, read_table, transaction
from src.reconciliation.comparator import record_hash
from tests.helpers import BASELINE, SCENARIO, make_batch, make_db, oracle, run, target_rows

STRATS = list(TARGETS)


def _norm_oracle(rows):
    return [tuple(int(x) if i == 3 else x for i, x in enumerate(r)) for r in rows]


def test_each_strategy_matches_oracle_on_scenario():
    expected = _norm_oracle(oracle(BASELINE, SCENARIO))
    for name in STRATS:
        conn = make_db()
        out = run(conn, name, make_batch(SCENARIO))
        assert out["status"] == "SUCCESS", out
        assert target_rows(conn, name) == expected, name


def test_counts_are_identical_across_incremental_and_partition():
    res = {}
    for name in ("incremental", "partition_replace"):
        conn = make_db()
        r = run(conn, name, make_batch(SCENARIO))["result"]
        res[name] = (r.inserted, r.updated, r.deleted, r.skipped_stale, r.noop_deletes)
    assert res["incremental"] == res["partition_replace"] == (1, 1, 2, 1, 1)


def test_late_arriving_update_never_overwrites_newer_state():
    for name in STRATS:
        conn = make_db()
        run(conn, name, make_batch(SCENARIO))
        o3 = [r for r in target_rows(conn, name) if r[0] == "O3"][0]
        assert o3[3] == 3000, f"{name}: stale event overwrote O3"


def test_late_event_in_second_batch_is_ignored_by_all_strategies():
    batch1 = [("UPDATE", "O1", "C1", "2026-01-01", 111, "PAID", "2026-03-01 00:00:00")]
    batch2 = [("UPDATE", "O1", "C1", "2026-01-01", 222, "PAID", "2026-02-01 00:00:00")]   # older, arrives later
    for name in STRATS:
        conn = make_db()
        run(conn, name, make_batch(batch1, "b1"), "r1")
        assert run(conn, name, make_batch(batch2, "b2"), "r2")["status"] == "SUCCESS"
        assert [r for r in target_rows(conn, name) if r[0] == "O1"][0][3] == 111, name


def test_multi_batch_equivalence():
    b1 = SCENARIO[:3]
    b2 = [("UPDATE", "O5", "C2", "2026-01-09", 6000, "PAID", "2026-03-01 00:00:00"),
          ("INSERT", "O6", "C1", "2026-01-03", 100, "PLACED", "2026-03-01 00:01:00")]
    expected = _norm_oracle(oracle(BASELINE, b1 + b2))
    for name in STRATS:
        conn = make_db()
        run(conn, name, make_batch(b1, "b1"), "r1")
        run(conn, name, make_batch(b2, "b2"), "r2")
        assert target_rows(conn, name) == expected, name


def test_partition_move_cleans_old_partition_and_fills_new():
    conn = make_db()
    run(conn, "partition_replace", make_batch(
        [("UPDATE", "O1", "C1", "2026-01-09", 1000, "PLACED", "2026-02-01 00:00:00")]), "r1")
    rows = {r[0]: r for r in target_rows(conn, "partition_replace")}
    assert rows["O1"][2] == "2026-01-09"
    manifest = dict((d, (b, a)) for d, b, a in conn.execute(
        "SELECT order_date, rows_before, rows_after FROM partition_manifest WHERE run_id='r1'"))
    assert manifest["2026-01-01"] == (2, 1)       # old partition rebuilt without O1 -> no stale record
    assert manifest["2026-01-09"] == (0, 1)       # new partition created


def test_empty_affected_partition_is_cleared():
    conn = make_db()
    run(conn, "partition_replace", make_batch(
        [("DELETE", "O4", None, None, None, None, "2026-02-01 00:00:00")]))
    assert conn.execute("SELECT COUNT(*) FROM target_partition WHERE order_date='2026-01-03'").fetchone()[0] == 0
    assert conn.execute("SELECT rows_after FROM partition_manifest WHERE order_date='2026-01-03'").fetchone()[0] == 0


def test_partition_strategy_touches_only_affected_partitions():
    conn = make_db()
    r = run(conn, "partition_replace", make_batch(
        [("UPDATE", "O3", "C2", "2026-01-02", 1, "PAID", "2026-02-01 00:00:00")]))["result"]
    assert r.extra["partitions_affected"] == 1 and r.extra["partitions_total"] == 3


def test_empty_change_set_is_a_noop():
    for name in STRATS:
        conn = make_db()
        before = target_rows(conn, name)
        assert run(conn, name, make_batch([]))["status"] == "SUCCESS"
        assert target_rows(conn, name) == before


def test_all_strategies_equivalent_on_generated_data_for_several_seeds():
    for seed in (1, 7, 42):
        tmp = Path(tempfile.mkdtemp())
        data_generator.generate(600, tmp, seed=seed)
        cfg = load_config(root=tmp)
        batch = prepare_batch(cfg, tmp / "baseline", tmp / "incoming")
        assert len(batch.rejected) > 0 and batch.rejected["reject_reason"].nunique() >= 4
        hashes = {}
        for name in STRATS:
            conn = connect(":memory:"); init_db(conn)
            with transaction(conn):
                repo.load_baseline(conn, batch.customers, batch.baseline)
            assert run(conn, name, batch)["status"] == "SUCCESS"
            hashes[name] = record_hash(read_table(conn, TARGETS[name]))
        assert len(set(hashes.values())) == 1, (seed, hashes)


def test_KNOWN_LIMITATION_no_tombstones_across_batches():
    """Documented gap (README / DESIGN_DECISIONS): a stale event arriving in a LATER batch for an order
    deleted in an EARLIER batch resurrects it under Incremental/Partition, but not under Full Refresh.
    This test pins the current behaviour; fixing it (a tombstone table) should flip the assertions."""
    b1 = [("DELETE", "O1", None, None, None, None, "2026-03-01 00:00:00")]
    b2 = [("UPDATE", "O1", "C1", "2026-01-01", 5, "PAID", "2026-02-01 00:00:00")]
    present = {}
    for name in STRATS:
        conn = make_db()
        run(conn, name, make_batch(b1, "b1"), "r1"); run(conn, name, make_batch(b2, "b2"), "r2")
        present[name] = any(r[0] == "O1" for r in target_rows(conn, name))
    assert present == {"full_refresh": False, "incremental": True, "partition_replace": True}
