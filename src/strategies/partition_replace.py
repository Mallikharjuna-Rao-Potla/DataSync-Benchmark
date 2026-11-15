from src.storage.database import ORDER_COLS, insert_df, read_table
from src.strategies.base_strategy import BaseStrategy, LoadResult, stage_changes
from src.transformation.transformer import apply_changes


class PartitionReplace(BaseStrategy):
    """Logical partitions = order_date (SQLite has no physical partitioning).
    1. affected partitions = NEW date of every upsert  U  OLD date of every touched order
       (so an order that moves date also cleans its old partition -- no stale rows),
    2. rebuild only those partitions in memory, 3. delete them, 4. insert rebuilt rows."""
    name = "partition_replace"

    def execute(self, source, conn):
        t = self.target
        stage_changes(conn, source.changes)
        conn.execute("DROP TABLE IF EXISTS tmp_partitions")
        conn.execute("CREATE TEMP TABLE tmp_partitions (order_date TEXT PRIMARY KEY)")
        conn.execute(f"""INSERT OR IGNORE INTO tmp_partitions
                         SELECT order_date FROM tmp_changes
                         WHERE operation!='DELETE' AND order_date IS NOT NULL""")
        conn.execute(f"""INSERT OR IGNORE INTO tmp_partitions
                         SELECT x.order_date FROM {t} x JOIN tmp_changes c ON c.order_id=x.order_id""")
        affected = [r[0] for r in conn.execute("SELECT order_date FROM tmp_partitions ORDER BY 1")]
        import pandas as pd
        existing = pd.read_sql_query(
            f"SELECT * FROM {t} WHERE order_date IN (SELECT order_date FROM tmp_partitions)", conn)
        rebuilt, stats = apply_changes(existing, source.changes)
        # rebuilt rows must stay inside the affected partitions (stale upserts add no row)
        conn.execute(f"DELETE FROM {t} WHERE order_date IN (SELECT order_date FROM tmp_partitions)")
        insert_df(conn, t, rebuilt, ORDER_COLS)
        before = existing.groupby("order_date").size().to_dict()
        after = rebuilt.groupby("order_date").size().to_dict()
        conn.executemany(
            "INSERT OR REPLACE INTO partition_manifest (run_id, order_date, rows_before, rows_after) VALUES (?,?,?,?)",
            [(source.run_id, d, int(before.get(d, 0)), int(after.get(d, 0))) for d in affected])
        total_parts = conn.execute(f"SELECT COUNT(DISTINCT order_date) FROM {t}").fetchone()[0]
        conn.execute("DROP TABLE tmp_partitions")
        conn.execute("DROP TABLE tmp_changes")
        return LoadResult(**stats, final_row_count=self._count(conn),
                          extra={"partitions_affected": len(affected),
                                 "partitions_total": int(total_parts),
                                 "rows_rewritten": len(rebuilt)})
