"""SQLite data tier: schema, connections, transactions, bulk helpers."""
import sqlite3
from contextlib import contextmanager
from pathlib import Path

TARGETS = {
    "full_refresh": "target_full_refresh",
    "incremental": "target_incremental",
    "partition_replace": "target_partition",
}
ORDER_COLS = ["order_id", "customer_id", "order_date", "amount", "status", "updated_at"]
EVENT_COLS = ["event_id", "order_id", "operation", "customer_id", "order_date",
              "amount", "status", "updated_at", "source_sequence"]


def _order_table(name):
    # amount is INTEGER paise (no float arithmetic on money).
    return f"""
    CREATE TABLE IF NOT EXISTS {name} (
        order_id    TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_date  TEXT NOT NULL,
        amount      INTEGER NOT NULL CHECK (amount >= 0),
        status      TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_{name}_date ON {name}(order_date);
    """


SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY, customer_name TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS order_events (          -- accepted event history (deduped by event_id)
        event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, operation TEXT NOT NULL,
        customer_id TEXT, order_date TEXT, amount INTEGER, status TEXT,
        updated_at TEXT NOT NULL, source_sequence INTEGER NOT NULL,
        batch_id TEXT, run_id TEXT, source_file TEXT, ingested_at TEXT);
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT NOT NULL, strategy TEXT NOT NULL, batch_id TEXT, status TEXT NOT NULL,
        started_at TEXT, ended_at TEXT, input_records INTEGER, valid_records INTEGER,
        rejected_records INTEGER, inserted INTEGER, updated INTEGER, deleted INTEGER,
        skipped_stale INTEGER, final_row_count INTEGER, duration_sec REAL,
        records_per_sec REAL, error_message TEXT, PRIMARY KEY (run_id, strategy));
    CREATE TABLE IF NOT EXISTS rejected_records (
        batch_id TEXT NOT NULL, run_id TEXT, source_file TEXT, record_type TEXT,
        source_row INTEGER, record_key TEXT, reject_reason TEXT NOT NULL, raw_record TEXT);
    CREATE INDEX IF NOT EXISTS idx_rejected_batch ON rejected_records(batch_id);
    CREATE TABLE IF NOT EXISTS partition_manifest (
        run_id TEXT NOT NULL, order_date TEXT NOT NULL, rows_before INTEGER, rows_after INTEGER,
        PRIMARY KEY (run_id, order_date));
    CREATE TABLE IF NOT EXISTS processed_batches (       -- idempotency ledger
        batch_id TEXT NOT NULL, strategy TEXT NOT NULL, run_id TEXT, processed_at TEXT,
        PRIMARY KEY (batch_id, strategy));
    """
    + _order_table("baseline_orders")
    + _order_table("staging_orders")
    + "".join(_order_table(t) for t in TARGETS.values())
)


def connect(path):
    """Autocommit connection; transactions are explicit (see `transaction`)."""
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn):
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn):
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def rows(df, cols):
    """DataFrame -> list of tuples of native Python values (NaN/NA -> None)."""
    out = df[cols].astype(object).where(df[cols].notna(), None)
    return out.values.tolist()


def insert_df(conn, table, df, cols, verb="INSERT"):
    if df is None or len(df) == 0:
        return 0
    ph = ",".join("?" * len(cols))
    conn.executemany(f"{verb} INTO {table} ({','.join(cols)}) VALUES ({ph})", rows(df, cols))
    return len(df)


def read_table(conn, table, order_by="order_id"):
    import pandas as pd
    return pd.read_sql_query(f"SELECT * FROM {table} ORDER BY {order_by}", conn)


def reset_targets(conn):
    """Put every target (and baseline copy) into the identical initial state = baseline."""
    for t in TARGETS.values():
        conn.execute(f"DELETE FROM {t}")
        conn.execute(f"INSERT INTO {t} SELECT * FROM baseline_orders")
    conn.execute("DELETE FROM staging_orders")
    conn.execute("DELETE FROM order_events")
    conn.execute("DELETE FROM processed_batches")
