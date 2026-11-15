from src.storage.database import ORDER_COLS, insert_df, read_table
from src.strategies.base_strategy import BaseStrategy, LoadResult
from src.transformation.transformer import apply_changes, collapse_events


class FullRefresh(BaseStrategy):
    """Rebuild the complete final dataset (baseline + ALL accepted event history), delete
    the whole target, insert everything. Simple and self-healing; cost ~ total data size."""
    name = "full_refresh"

    def execute(self, source, conn):
        baseline = read_table(conn, "baseline_orders")
        history = collapse_events(read_table(conn, "order_events", order_by="event_id"))
        final, stats = apply_changes(baseline, history)
        conn.execute(f"DELETE FROM {self.target}")
        insert_df(conn, self.target, final, ORDER_COLS)
        # NB: counts are relative to the baseline (what was rebuilt), not to the old target.
        return LoadResult(**stats, final_row_count=len(final),
                          extra={"rows_written": len(final)})
