from src.strategies.base_strategy import BaseStrategy, LoadResult, stage_changes


class IncrementalMerge(BaseStrategy):
    """Apply only the changed orders with set-based SQL (UPSERT + DELETE).
    A change older than the stored row's updated_at is stale and never overwrites it."""
    name = "incremental"

    def execute(self, source, conn):
        t = self.target
        stage_changes(conn, source.changes)
        q = lambda sql: conn.execute(sql).fetchone()[0]       # noqa: E731
        # classify BEFORE mutating, for metrics
        inserted = q(f"""SELECT COUNT(*) FROM tmp_changes c WHERE c.operation!='DELETE'
                         AND NOT EXISTS (SELECT 1 FROM {t} x WHERE x.order_id=c.order_id)""")
        updated = q(f"""SELECT COUNT(*) FROM tmp_changes c JOIN {t} x ON x.order_id=c.order_id
                        WHERE c.operation!='DELETE' AND c.updated_at >= x.updated_at""")
        deleted = q(f"""SELECT COUNT(*) FROM tmp_changes c JOIN {t} x ON x.order_id=c.order_id
                        WHERE c.operation='DELETE' AND c.updated_at >= x.updated_at""")
        noop_del = q(f"""SELECT COUNT(*) FROM tmp_changes c WHERE c.operation='DELETE'
                         AND NOT EXISTS (SELECT 1 FROM {t} x WHERE x.order_id=c.order_id)""")
        stale = q(f"""SELECT COUNT(*) FROM tmp_changes c JOIN {t} x ON x.order_id=c.order_id
                      WHERE c.updated_at < x.updated_at""")
        conn.execute(f"""DELETE FROM {t} WHERE EXISTS (      -- PK lookup into tmp_changes, not a rescan
                           SELECT 1 FROM tmp_changes c WHERE c.order_id = {t}.order_id
                           AND c.operation='DELETE' AND c.updated_at >= {t}.updated_at)""")
        conn.execute(f"""INSERT INTO {t} (order_id, customer_id, order_date, amount, status, updated_at)
                         SELECT order_id, customer_id, order_date, amount, status, updated_at
                         FROM tmp_changes WHERE operation!='DELETE' AND true
                         ON CONFLICT(order_id) DO UPDATE SET
                           customer_id=excluded.customer_id, order_date=excluded.order_date,
                           amount=excluded.amount, status=excluded.status,
                           updated_at=excluded.updated_at
                         WHERE excluded.updated_at >= {t}.updated_at""")
        conn.execute("DROP TABLE tmp_changes")
        return LoadResult(inserted=inserted, updated=updated, deleted=deleted,
                          skipped_stale=stale, noop_deletes=noop_del,
                          final_row_count=self._count(conn))
