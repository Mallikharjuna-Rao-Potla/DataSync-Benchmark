from src.storage.database import TARGETS
from src.strategies import get_strategy
from src.strategies.incremental_merge import IncrementalMerge
from src.strategies.full_refresh import FullRefresh
from src.strategies.partition_replace import PartitionReplace
from src.orchestration.pipeline import run_strategy
from tests.helpers import SCENARIO, make_batch, make_db, run, target_rows

STRATS = list(TARGETS)


def test_rerunning_same_batch_is_skipped_and_changes_nothing():
    for name in STRATS:
        conn = make_db()
        batch = make_batch(SCENARIO)
        assert run(conn, name, batch, "r1")["status"] == "SUCCESS"
        after_first = target_rows(conn, name)
        assert run(conn, name, batch, "r2")["status"] == "SKIPPED_DUPLICATE_BATCH"
        assert target_rows(conn, name) == after_first
        assert conn.execute("SELECT COUNT(*) FROM processed_batches WHERE strategy=?", (name,)).fetchone()[0] == 1
        assert conn.execute("SELECT status FROM pipeline_runs WHERE run_id='r2'").fetchone()[0] == "SKIPPED_DUPLICATE_BATCH"


def _failing(cls):
    class Boom(cls):
        def execute(self, source, conn):
            super().execute(source, conn)               # do ALL the work ...
            raise RuntimeError("simulated crash before commit")   # ... then die
    return Boom()


def test_crash_rolls_back_data_history_and_idempotency_marker():
    for cls in (FullRefresh, IncrementalMerge, PartitionReplace):
        conn = make_db()
        before = target_rows(conn, cls.name)
        out = run_strategy(conn, _failing(cls), make_batch(SCENARIO), "r1")
        assert out["status"] == "FAILED" and "simulated crash" in out["error"]
        assert target_rows(conn, cls.name) == before, cls.name
        assert conn.execute("SELECT COUNT(*) FROM processed_batches").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0] == 0
        if cls is PartitionReplace:
            assert conn.execute("SELECT COUNT(*) FROM partition_manifest").fetchone()[0] == 0
        row = conn.execute("SELECT status, error_message FROM pipeline_runs WHERE run_id='r1'").fetchone()
        assert row[0] == "FAILED" and "simulated crash" in row[1]          # failure is still recorded


def test_retry_after_failure_succeeds_and_matches_clean_run():
    for cls in (FullRefresh, IncrementalMerge, PartitionReplace):
        conn = make_db()
        run_strategy(conn, _failing(cls), make_batch(SCENARIO), "r1")
        assert run(conn, cls.name, make_batch(SCENARIO), "r2")["status"] == "SUCCESS"
        clean = make_db()
        run(clean, cls.name, make_batch(SCENARIO), "r1")
        assert target_rows(conn, cls.name) == target_rows(clean, cls.name)


def test_database_constraint_failure_mid_load_rolls_back():
    # amount CHECK (>= 0) fails on the 2nd upsert after a delete already happened in the txn
    bad = [("DELETE", "O2", None, None, None, None, "2026-02-01 00:00:00"),
           ("UPDATE", "O1", "C1", "2026-01-01", -5, "PAID", "2026-02-01 00:00:00")]
    for name in STRATS:
        conn = make_db()
        before = target_rows(conn, name)
        out = run(conn, name, make_batch(bad))
        assert out["status"] == "FAILED", name
        assert target_rows(conn, name) == before, name


def test_failure_of_one_strategy_does_not_affect_the_others():
    conn = make_db()
    run_strategy(conn, _failing(IncrementalMerge), make_batch(SCENARIO), "r1")
    assert run(conn, "full_refresh", make_batch(SCENARIO), "r1")["status"] == "SUCCESS"
    assert run(conn, "partition_replace", make_batch(SCENARIO), "r1")["status"] == "SUCCESS"
    assert target_rows(conn, "full_refresh") == target_rows(conn, "partition_replace")


def test_same_input_twice_in_fresh_dbs_gives_same_result():
    a, b = make_db(), make_db()
    run(a, "incremental", make_batch(SCENARIO)); run(b, "incremental", make_batch(SCENARIO))
    assert target_rows(a, "incremental") == target_rows(b, "incremental")
