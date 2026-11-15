# Architecture

```
                 +---------------- TIER 1: PRESENTATION ----------------+
                 |  run_pipeline.py (CLI)       dashboard/app.py (read) |
                 +------------------------+-----------------------------+
                                          v
+----------------------------- TIER 2: APPLICATION (src/) -------------------------------+
| orchestration/pipeline.py   sequences everything; owns the transaction per strategy      |
|   ingestion -> validation -> transformation ->  strategies/ (Strategy pattern)           |
|   source_reader  validator    transformer        FullRefresh | IncrementalMerge |        |
|   data_generator schema_val.  apply_changes()    PartitionReplace                         |
|                                   v                                                       |
|   reconciliation/comparator   monitoring/(logger, run_tracker)   benchmark/performance    |
+------------------------------------------+-----------------------------------------------+
                                           v
+------------------------------- TIER 3: DATA --------------------------------------------+
| SQLite  data/warehouse/benchmark.db                                                      |
|   raw-ish: customers, baseline_orders, order_events                                      |
|   staging: staging_orders           curated: target_full_refresh / _incremental / _partition |
|   metadata: pipeline_runs, rejected_records, partition_manifest, processed_batches       |
| Files: data/raw (CSV), data/staging + data/quarantine (Parquet), data/reports (JSON/CSV) |
+------------------------------------------------------------------------------------------+
```

## Per-strategy transaction (the reliability core)

```
BEGIN
  save accepted events (INSERT OR IGNORE)  +  save rejected records
  -- timer starts --
  strategy.execute()          # never commits
  mark processed_batches(batch_id, strategy)
COMMIT  -- timer stops --     # any exception -> ROLLBACK of all of the above
then: pipeline_runs row updated (SUCCESS | FAILED | SKIPPED_DUPLICATE_BATCH)
```

## Strategy internals
- **Full Refresh**: baseline + all accepted events -> `apply_changes` -> `DELETE` target -> bulk insert.
- **Incremental Merge**: net changes into a TEMP table (PK on order_id) -> classify for metrics ->
  `DELETE ... WHERE EXISTS` (stale-guarded) -> `INSERT ... ON CONFLICT DO UPDATE ... WHERE excluded.updated_at >= target.updated_at`.
- **Partition Replace**: affected dates = new date of each upsert U old date of each touched order ->
  read those partitions -> `apply_changes` in pandas -> delete partitions -> insert rebuilt rows -> `partition_manifest`.
