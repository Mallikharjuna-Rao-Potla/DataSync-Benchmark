"""Small persistence helpers used by the orchestrator."""
from datetime import datetime, timezone

from src.storage.database import EVENT_COLS, ORDER_COLS, TARGETS, insert_df


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def save_events(conn, events, run_id, batch_id, source_file):
    """Accepted-event history; INSERT OR IGNORE keeps it idempotent on event_id."""
    if len(events) == 0:
        return 0
    e = events.copy()
    e["batch_id"], e["run_id"], e["source_file"], e["ingested_at"] = batch_id, run_id, source_file, now_iso()
    cols = EVENT_COLS + ["batch_id", "run_id", "source_file", "ingested_at"]
    return insert_df(conn, "order_events", e, cols, verb="INSERT OR IGNORE")


def save_rejected(conn, rejected, run_id, batch_id):
    conn.execute("DELETE FROM rejected_records WHERE batch_id=?", (batch_id,))
    if len(rejected) == 0:
        return 0
    r = rejected.copy()
    r["batch_id"], r["run_id"] = batch_id, run_id
    cols = ["batch_id", "run_id", "source_file", "record_type", "source_row",
            "record_key", "reject_reason", "raw_record"]
    return insert_df(conn, "rejected_records", r, cols)


def is_processed(conn, batch_id, strategy):
    return conn.execute("SELECT 1 FROM processed_batches WHERE batch_id=? AND strategy=?",
                        (batch_id, strategy)).fetchone() is not None


def mark_processed(conn, batch_id, strategy, run_id):
    conn.execute("INSERT INTO processed_batches (batch_id, strategy, run_id, processed_at) VALUES (?,?,?,?)",
                 (batch_id, strategy, run_id, now_iso()))


def load_baseline(conn, customers, baseline_orders):
    """(Re)initialise reference data and put every target into the identical baseline state."""
    conn.execute("DELETE FROM customers")
    conn.execute("DELETE FROM baseline_orders")
    insert_df(conn, "customers", customers, ["customer_id", "customer_name", "created_at"])
    insert_df(conn, "baseline_orders", baseline_orders, ORDER_COLS)
    from src.storage.database import reset_targets
    reset_targets(conn)


def replace_staging(conn, expected):
    conn.execute("DELETE FROM staging_orders")
    insert_df(conn, "staging_orders", expected, ORDER_COLS)
