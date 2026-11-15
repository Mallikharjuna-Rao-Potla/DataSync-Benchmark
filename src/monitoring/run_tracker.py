"""Run history. Writes happen in autocommit mode, OUTSIDE the load transaction, so a FAILED
run is still recorded after the load itself has been rolled back."""
import uuid
from datetime import datetime, timezone

from src.storage.repositories import now_iso


def new_run_id():
    return f"run_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"


class RunTracker:
    def __init__(self, conn):
        self.conn = conn

    def start(self, run_id, strategy, batch_id, input_records, valid_records, rejected_records):
        self.conn.execute(
            """INSERT OR REPLACE INTO pipeline_runs (run_id, strategy, batch_id, status, started_at,
               input_records, valid_records, rejected_records) VALUES (?,?,?,?,?,?,?,?)""",
            (run_id, strategy, batch_id, "RUNNING", now_iso(), input_records, valid_records,
             rejected_records))

    def finish(self, run_id, strategy, status, result=None, duration=None, error=None, valid=None):
        r = result
        rps = (valid / duration) if (valid and duration) else None
        self.conn.execute(
            """UPDATE pipeline_runs SET status=?, ended_at=?, inserted=?, updated=?, deleted=?,
               skipped_stale=?, final_row_count=?, duration_sec=?, records_per_sec=?, error_message=?
               WHERE run_id=? AND strategy=?""",
            (status, now_iso(), getattr(r, "inserted", None), getattr(r, "updated", None),
             getattr(r, "deleted", None), getattr(r, "skipped_stale", None),
             getattr(r, "final_row_count", None), duration, rps, error, run_id, strategy))

    def history(self, limit=50):
        import pandas as pd
        return pd.read_sql_query("SELECT * FROM pipeline_runs ORDER BY started_at DESC, rowid DESC LIMIT ?",
                                 self.conn, params=(limit,))
