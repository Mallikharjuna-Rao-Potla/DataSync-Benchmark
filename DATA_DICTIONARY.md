# Data Dictionary

All amounts are **integer paise** (INR x 100). Timestamps are `YYYY-MM-DD HH:MM:SS`, dates `YYYY-MM-DD`.

## Source files (CSV, all columns read as text)
| File | Columns |
|---|---|
| `baseline/customers.csv` | customer_id, customer_name, created_at |
| `baseline/orders.csv` | order_id, customer_id, order_date, amount (rupees, text), status, updated_at |
| `incoming/order_events.csv` | event_id, order_id, operation, customer_id, order_date, amount, status, updated_at, source_sequence |

Source dates may be `YYYY-MM-DD` or `DD-MM-YYYY`; amounts are rupee strings (`"1500"`, `"1,234.56"`).
INSERT/UPDATE carry the full row; DELETE needs only `event_id`, `order_id`, `operation`, `updated_at`, `source_sequence`.

## Warehouse tables (SQLite)
| Table | Purpose | Key |
|---|---|---|
| customers | reference data | customer_id |
| baseline_orders | validated initial snapshot | order_id |
| order_events | accepted event history (deduped), with lineage `batch_id, run_id, source_file, ingested_at` | event_id |
| staging_orders | validated expected final state (written after reconciliation) | order_id |
| target_full_refresh / target_incremental / target_partition | the three alternative outputs (same schema, same index on order_date) | order_id |
| pipeline_runs | one row per (run_id, strategy): status, counts, duration, error | run_id, strategy |
| rejected_records | quarantine: batch_id, source_file, source_row, record_key, reject_reason, raw_record (JSON) | - |
| partition_manifest | per run, per affected date: rows_before / rows_after | run_id, order_date |
| processed_batches | idempotency ledger | batch_id, strategy |

Order columns: `order_id TEXT PK, customer_id TEXT NOT NULL, order_date TEXT NOT NULL,
amount INTEGER NOT NULL CHECK(amount>=0), status TEXT NOT NULL, updated_at TEXT NOT NULL`.

## Reject reasons
`MISSING_ORDER_ID`, `MISSING_EVENT_ID`, `INVALID_OPERATION`, `INVALID_TIMESTAMP`, `INVALID_SEQUENCE`,
`MISSING_REQUIRED_FIELD`, `INVALID_DATE`, `INVALID_AMOUNT`, `NEGATIVE_AMOUNT`, `INVALID_CUSTOMER`,
`INVALID_STATUS`, `DUPLICATE_EVENT`, `DUPLICATE_ORDER_ID` (baseline). Missing *columns* are not record
rejects - they fail the batch (`SchemaError`).

## Statuses
`PLACED, PAID, SHIPPED, DELIVERED, CANCELLED` (configurable). Input is trimmed and upper-cased.

## Files produced
`data/staging/valid_events.parquet|csv`, `data/staging/transformed_orders.parquet|csv`,
`data/quarantine/rejected_records.parquet|csv`, `data/reports/{data_quality_report.json,
reconciliation_report.json, performance_report.csv, benchmark_report.csv}`, `data/logs/pipeline.log`.
