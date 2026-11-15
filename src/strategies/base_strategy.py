"""Strategy pattern: every loading strategy has the same interface and the same inputs."""
from dataclasses import dataclass, field

from src.storage.database import TARGETS


@dataclass
class SourceData:
    """What a strategy receives. `changes` = this batch's net change per order."""
    changes: object                # DataFrame (CHANGE_COLS), one row per order_id
    run_id: str = "run_adhoc"
    batch_id: str = "batch_adhoc"


@dataclass
class LoadResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    skipped_stale: int = 0
    noop_deletes: int = 0
    final_row_count: int = 0
    extra: dict = field(default_factory=dict)


class BaseStrategy:
    name = "base"

    @property
    def target(self):
        return TARGETS[self.name]

    def execute(self, source, conn):
        """Mutate the target through `conn`. MUST NOT commit: the orchestrator owns the
        transaction so the data change and the idempotency marker commit atomically."""
        raise NotImplementedError

    def _count(self, conn):
        return conn.execute(f"SELECT COUNT(*) FROM {self.target}").fetchone()[0]


def stage_changes(conn, changes):
    """Load the batch's net changes into a TEMP table (inside the caller's transaction)."""
    from src.storage.database import insert_df
    conn.execute("DROP TABLE IF EXISTS tmp_changes")
    conn.execute("""CREATE TEMP TABLE tmp_changes (
        event_id TEXT, order_id TEXT PRIMARY KEY, operation TEXT, customer_id TEXT,
        order_date TEXT, amount INTEGER, status TEXT, updated_at TEXT, source_sequence INTEGER)""")
    cols = ["event_id", "order_id", "operation", "customer_id", "order_date", "amount",
            "status", "updated_at", "source_sequence"]
    insert_df(conn, "tmp_changes", changes, cols)
