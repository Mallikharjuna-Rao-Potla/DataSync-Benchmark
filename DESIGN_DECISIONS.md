# Design Decisions

1. **SQLite, not DuckDB** - stdlib, transactional, easy to explain; the transactional guarantees
   (rollback, atomic marker) are what the problem needs.
2. **CDC-style events on a baseline** - gives meaningful inserts, updates, deletes, duplicates and late data.
3. **Validation separate from transformation** - validator decides *acceptability*; transformer
   *shapes* accepted rows. Both share the same parsers, so "valid" always means "transformable".
4. **One pipeline, three strategies** - ingestion/validation/transformation run once; strategies receive
   identical `SourceData`. Adding a strategy = one new class in `strategies/`.
5. **Reconciliation is mandatory** - success requires all targets to equal the expected state and each other (hash of every record).
6. **Integer paise** - exact money arithmetic, rounding is explicit (half-up) in one place.
7. **Net change per order** - `collapse_events` keeps the event with the greatest `(updated_at, source_sequence)`;
   it makes in-batch out-of-order delivery irrelevant and keeps the three strategies on identical input.
8. **Stale rule: `change.updated_at < row.updated_at` -> ignore** (ties apply). Implemented twice on purpose
   (SQL for Incremental, pandas for Full/Partition), so reconciliation cross-checks two implementations.
9. **Marker in the same transaction as the data** - otherwise a crash between "write" and "mark" creates
   either duplicate processing or a lost batch.
10. **Run history written outside the load transaction** - a failed run must remain visible after rollback.
11. **Fair benchmark** - identical inputs; fresh DB file copy per repetition (so no run benefits from a warmed
    or already-merged target); timed region includes COMMIT; median of N. Event-history and quarantine writes
    happen before the timer (identical for all strategies).
12. **Equivalent indexes** on all three targets (PK + order_date) so no strategy is advantaged by schema.

## Bug found while benchmarking
First Incremental implementation deleted with `WHERE order_id IN (SELECT ... correlated ...)`, which
rescanned the change table per target row: 1.53 s at 10k vs 0.09 s for Full Refresh. Rewritten as an
indexed `EXISTS` lookup -> 0.03 s. The benchmark exposed a query-plan defect rather than a property of the strategy.

## Known gaps / what I would do next
- **Tombstones**: keep `deleted_orders(order_id, deleted_at)` so a stale event in a later batch cannot resurrect a
  deleted order (currently only handled inside a batch and against existing rows).
- **Partition Replace in SQL** (`INSERT ... SELECT` per partition) to separate strategy cost from the
  pandas round-trip; compare again.
- Chunked/streaming ingestion for sources larger than memory; schema evolution handling.
- Backfill CLI (`--partitions 2026-01-01..2026-01-31`) re-running selected dates from event history.
