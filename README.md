# DataSync Benchmark
### Comparative Data Loading and Reconciliation Framework

A configuration-driven data engineering framework for evaluating three data loading strategies — **Full Refresh, Incremental Merge, and Partition Replace** — using a common input dataset, transactional processing, and record-level reconciliation.

The framework demonstrates how different loading strategies handle data changes, duplicate events, late-arriving records, data quality issues, and recovery scenarios while maintaining consistent analytical results.

---

## 1. Project Overview

DataSync Benchmark is a modular ETL/ELT-style data pipeline designed to compare the correctness and performance of different data loading approaches.

The system generates synthetic e-commerce datasets containing customer records, baseline orders, and incoming order events. It processes the same validated input through three independent loading strategies and verifies whether all strategies produce an equivalent final dataset.

### Key Features

- Three-tier application architecture.
- Configurable synthetic data generation.
- CSV-based source ingestion.
- Record-level data validation and quarantine.
- Data transformation and event deduplication.
- Three interchangeable loading strategies.
- Transactional execution and rollback.
- Batch-level idempotency.
- Late-arriving event handling.
- Record-level reconciliation.
- Performance benchmarking.
- Execution monitoring and reporting.
- Automated test coverage.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python 3.12 |
| Data Processing | Pandas, NumPy |
| Database | SQLite |
| Source Data | CSV |
| Staging and Quarantine | Parquet / CSV |
| Configuration | YAML |
| Dashboard | Streamlit |
| Testing | Pytest |
| Hashing | SHA-256 |

## 3. System Architecture

The application follows a three-tier architecture that separates presentation, application processing, and data storage.

```text
                 DATA SYNC BENCHMARK
                         |
          +--------------+--------------+
          |                             |
     TIER 1: PRESENTATION
          |
     +----+----------------------+
     |                           |
  CLI Interface            Streamlit Dashboard
  run_pipeline.py          dashboard/app.py
     |                           |
     +-------------+-------------+
                   |
          TIER 2: APPLICATION
                   |
             Orchestration
                   |
          +--------+---------+
          |                  |
       Ingestion          Monitoring
          |
       Validation
          |
      Transformation
          |
      Change Detection
          |
     +-----+---------+
     |       |       |
    Full  Incremental Partition
   Refresh   Merge   Replace
     |       |       |
     +-------+-------+
             |
        Reconciliation
             |
         Benchmarking
             |
          TIER 3: DATA
             |
     +-------+----------------+
     |            |           |
   SQLite       CSV        Parquet
  Warehouse   Source Data  Staging /
                          Quarantine
             |
        JSON Reports
```

### Tier 1 — Presentation Layer

Provides interfaces for interacting with the pipeline.

**CLI (`run_pipeline.py`)**
- Starts pipeline execution.
- Selects loading strategies.
- Generates datasets.
- Runs benchmarks.
- Controls execution parameters.

**Streamlit Dashboard (`dashboard/app.py`)**
- Displays pipeline execution information.
- Provides visibility into monitoring and benchmark outputs.
- Supports read-only inspection of available results.

### Tier 2 — Application Layer

Contains the core business logic and pipeline orchestration.

Responsibilities include:

- Source ingestion.
- Data validation.
- Data transformation.
- Event processing.
- Loading strategy execution.
- Transaction management.
- Reconciliation.
- Performance measurement.
- Execution monitoring.

### Tier 3 — Data Layer

Manages persistent data and pipeline outputs.

| Storage | Purpose |
|---|---|
| SQLite | Analytical warehouse and execution metadata |
| CSV | Baseline and incoming source datasets |
| Parquet | Staging and rejected records |
| JSON | Dataset metadata and reconciliation reports |
| CSV | Benchmark and performance reports |

---

## 4. Pipeline Execution Flow

The pipeline processes data through a common sequence of stages.

```text
Dataset Generation
        |
        v
Source Ingestion
        |
        v
Schema Validation
        |
        v
Record-Level Validation
        |
        +------ Invalid Records
        |              |
        |              v
        |          Quarantine
        |
        v
Data Transformation
        |
        v
Event Deduplication
        |
        v
Change Detection
        |
        v
Loading Strategy
        |
        +----------+-----------+
        |          |           |
     Full       Incremental  Partition
    Refresh       Merge      Replace
        |          |           |
        +----------+-----------+
                   |
                   v
             Reconciliation
                   |
                   v
           Performance Reports
                   |
                   v
             Monitoring
```

### Pipeline Components

| Stage | Module | Responsibility |
|---|---|---|
| Ingestion | `src/ingestion/source_reader.py` | Reads and checks source CSV files |
| Generation | `src/ingestion/data_generator.py` | Generates deterministic synthetic datasets |
| Validation | `src/validation/validator.py` | Validates records and identifies invalid data |
| Transformation | `src/transformation/transformer.py` | Normalizes and transforms incoming events |
| Strategies | `src/strategies/` | Implements the three loading approaches |
| Orchestration | `src/orchestration/pipeline.py` | Coordinates pipeline execution |
| Reconciliation | `src/reconciliation/comparator.py` | Compares strategy outputs |
| Benchmarking | `src/benchmark/performance.py` | Measures execution performance |
| Monitoring | `src/monitoring/` | Records execution status and metrics |

---

## 5. Data Model and Source Structure

The framework uses a simplified Change Data Capture (CDC) model.

### Source Datasets

**Baseline Orders**

Represents the initial order snapshot.

`baseline/orders.csv`

**Customers**

Contains customer reference information.

`baseline/customers.csv`

**Incoming Order Events**

Contains changes that must be applied to the baseline.

`incoming/order_events.csv`

Supported operations:

- `INSERT`
- `UPDATE`
- `DELETE`

Each event contains metadata such as:

- `event_id`
- `order_id`
- `source_sequence`
- `updated_at`
- `operation`

INSERT and UPDATE events contain the business fields required to create or update an order. DELETE events require the order key and event metadata.

### Synthetic Data Generation

The generator supports configurable dataset sizes and uses a fixed random seed for reproducibility.

Generated scenarios include:

- Duplicate event deliveries.
- Out-of-order events.
- Late-arriving updates.
- Different date formats.
- Lowercase status values.
- Invalid customer references.
- Missing order identifiers.
- Invalid operations.
- Negative and non-numeric amounts.
- Invalid dates and missing required fields.

Event IDs are namespaced by batch identity to avoid collisions between independent generated batches. When generation parameters are identical, the default batch identity is deterministic.

For independent batches with identical generation parameters, callers should supply distinct explicit batch IDs.

---

## 6. Data Loading Strategies

The central purpose of this project is to compare three data loading approaches using the same validated and transformed input.

### 6.1 Full Refresh

Full Refresh rebuilds the target dataset using the baseline and accumulated accepted event history.

**Processing flow:**

1. Read baseline data.
2. Retrieve accepted event history.
3. Reconstruct the expected current dataset.
4. Replace the existing target contents.
5. Record execution statistics.

**Characteristics:**
- Simple implementation.
- Reprocesses the complete dataset.
- Does not depend on identifying only changed records.
- Useful for complete reloads and recovery.

**Trade-off:** Processing cost increases with the total dataset size.

### 6.2 Incremental Merge

Incremental Merge applies only the latest valid changes to existing target records.

**Processing flow:**

1. Identify new and updated records.
2. Identify delete operations.
3. Compare incoming timestamps with stored timestamps.
4. Apply eligible changes.
5. Ignore stale updates.
6. Record affected-row statistics.

**Characteristics:**
- Processes changed records rather than rebuilding the entire target.
- Uses set-based database operations.
- Supports insert, update, and delete operations.
- Reduces unnecessary data rewriting.

**Trade-off:** Requires reliable change detection, event ordering, and idempotency controls.

### 6.3 Partition Replace

Partition Replace rebuilds only the affected logical date partitions.

The partition key in this implementation is `order_date`.

**Processing flow:**

1. Identify affected order dates from incoming changes.
2. Identify the previous dates of existing orders being updated or deleted.
3. Read the affected partitions.
4. Apply changes to the selected data.
5. Replace the affected partitions.
6. Record partition-level statistics.

**Important:** SQLite does not provide native physical table partitioning. This implementation uses logical partitions through `order_date` filtering.

When an order moves from one date to another, both the old and new partitions must be considered to prevent stale records.

**Characteristics:**
- Limits rewriting to affected date ranges.
- Supports date-based replacement and backfills.
- Can be efficient when incoming changes are concentrated in a small number of dates.

**Trade-off:** Performance depends heavily on how changes are distributed across partitions.

### Strategy Comparison

| Attribute | Full Refresh | Incremental Merge | Partition Replace |
|---|---|---|---|
| Processing scope | Entire dataset | Changed records | Affected partitions |
| Complexity | Low | Medium | High |
| Data rewriting | High | Low | Depends on affected partitions |
| Late-event handling | Supported | Supported | Supported |
| Recovery | Complete reload | Transactional retry | Transactional partition retry |
| Suitable workload | Complete reloads | Frequent small changes | Date-localized changes |

---

## 7. Data Quality and Transformation

Data quality checks are performed before records reach the warehouse.

### Validation

Invalid records are separated into quarantine storage with a rejection reason.

Examples include:

- Null order identifiers.
- Negative amounts.
- Non-numeric amounts.
- Unknown customer references.
- Invalid operations.
- Invalid dates.
- Invalid timestamps.
- Invalid order statuses.
- Missing required fields.

A record with invalid business data is not silently discarded.

A missing source file, malformed CSV, or missing required column causes the batch to fail rather than loading incomplete input.

### Transformation

The transformation layer standardizes incoming data.

| Field | Transformation |
|---|---|
| Order Date | Normalized to ISO date format |
| Amount | Converted to integer paise |
| Timestamp | Normalized to a consistent timestamp representation |
| Operation | Standardized operation values |
| Status | Standardized status values |
| Events | Collapsed into the latest eligible change per order |

Amounts are stored as integer paise to avoid floating-point arithmetic errors.

---

## 8. Data Correctness and Reconciliation

Correctness is a primary requirement of this framework.

The objective is not simply to load records successfully. All three strategies must produce the same final business state.

### Reconciliation Process

The framework compares each strategy's output against an independently reconstructed expected state.

Checks include:

- Total row counts.
- Primary-key uniqueness.
- Missing records.
- Extra records.
- Field-level value differences.
- Aggregate comparisons.
- SHA-256 record-level checksums.

The expected state is reconstructed independently from the baseline and accepted event history.

A successful reconciliation indicates that the selected loading strategies produced equivalent results for the tested input.

### Event Ordering

Within a batch, the winning event is determined by:

`(updated_at, source_sequence)`

The event with the greatest ordering tuple takes precedence.

This prevents file arrival order from determining the final state.

### Late-Arriving Events

An event older than the stored record's timestamp is ignored.

The implementation applies this rule in both SQL and pandas transformation logic.

### Duplicate Events

The first delivery of an event ID is accepted. Subsequent deliveries of the same event ID are quarantined with the `DUPLICATE_EVENT` rejection reason.

### Date Changes

Partition Replace considers both the old and new order dates when an order changes its date.

This prevents stale rows from remaining in the previous logical partition.

---

## 9. Idempotency and Failure Recovery

The framework includes batch-level idempotency and transactional recovery.

### Batch Identification

A batch is identified using a SHA-256 hash of its incoming event file.

The processed batch ledger tracks which strategies have successfully processed each batch.

### Transaction Management

The following operations are performed within a single database transaction:

1. Accepted event history write.
2. Rejected record write.
3. Strategy execution.
4. Processed batch ledger update.

If execution fails, the transaction is rolled back.

This prevents partially loaded data or incomplete idempotency markers from remaining in the warehouse.

### Retry Behaviour

| Scenario | Expected Behaviour |
|---|---|
| Same batch processed again | Skipped as duplicate |
| Database error during loading | Transaction rollback |
| Retry after failure | Processing can succeed |
| Invalid records | Stored in quarantine |
| Missing source file | Batch fails |
| Missing required column | Schema validation failure |
| Header-only input | Controlled no-op |
| One strategy fails | Other selected strategies remain isolated |

Failed execution records are stored outside the loading transaction so that failure information survives rollback.

---

## 10. Benchmarking Methodology

The benchmarking module evaluates the execution performance of the three loading strategies.

### Benchmark Parameters

- Dataset size.
- Number of repetitions.
- Random seed.
- Number of affected days.
- Loading strategy.

### Measurement Approach

For fair comparison:

1. Each strategy receives equivalent input data.
2. Each repetition starts from a fresh copy of the same initial database state.
3. Execution is measured independently.
4. Multiple repetitions are performed.
5. Median execution time is reported.

### Performance Metrics

| Metric | Description |
|---|---|
| Execution Time | Time taken by the loading strategy |
| Throughput | Number of valid input events processed per second |
| Rows Written | Number of target rows rewritten or affected |
| Final Row Count | Total rows after loading |
| Partitions Affected | Number of logical partitions replaced |
| Database Size | Resulting database size |
| Reconciliation | Whether the resulting dataset matches the expected state |

Benchmark timings are machine-dependent. Results should be interpreted in the context of the hardware, Python environment, database state, and workload distribution.

### Benchmark Scenarios

The project supports different workload sizes, including:

- 100 records.
- 10,000 records.
- 100,000 records.
- 500,000 records.

It also supports localized workloads through the `--hot-days` parameter.

A localized workload is useful for evaluating whether Partition Replace benefits when changes are concentrated in a small number of date partitions.

### Performance Reports

Benchmark outputs are stored in the configured reports directory.

The repository also contains:

- `docs/benchmark_results.csv` — recorded benchmark measurements.
- `docs/sample_run/` — sample pipeline reports.

Use the recorded results as experimental measurements, not universal performance guarantees.

---

## 11. Project Structure

```text
DataSync-Benchmark/
│
├── config/
│   ├── config.yaml
│   └── config_validation.yaml
│
├── dashboard/
│   └── app.py
│
├── docs/
│   ├── benchmark_results.csv
│   └── sample_run/
│
├── src/
│   ├── ingestion/
│   │   ├── data_generator.py
│   │   └── source_reader.py
│   │
│   ├── validation/
│   │   └── validator.py
│   │
│   ├── transformation/
│   │   └── transformer.py
│   │
│   ├── strategies/
│   │   ├── base_strategy.py
│   │   ├── full_refresh.py
│   │   ├── incremental_merge.py
│   │   └── partition_replace.py
│   │
│   ├── orchestration/
│   │   └── pipeline.py
│   │
│   ├── reconciliation/
│   │   └── comparator.py
│   │
│   ├── benchmark/
│   │   └── performance.py
│   │
│   ├── monitoring/
│   │
│   └── storage/
│       └── database.py
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_validation.py
│   ├── test_transformation.py
│   ├── test_strategies.py
│   ├── test_reconciliation.py
│   ├── test_pipeline_e2e.py
│   ├── test_idempotency_recovery.py
│   └── helpers.py
│
├── data/
│   ├── raw/
│   ├── staging/
│   ├── quarantine/
│   ├── warehouse/
│   ├── reports/
│   └── logs/
│
├── run_pipeline.py
├── test_generator.py
├── requirements.txt
├── pytest.ini
├── DATA_DICTIONARY.md
├── DESIGN_DECISIONS.md
└── README.md
```

*Note: The structure above describes the main project components. Actual generated data and report files depend on the configured paths and execution.*

---

## 12. Installation and Setup

### Prerequisites

- Python 3.12
- pip
- Git

### Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd DataSync-Benchmark
```

### Step 2: Create a Virtual Environment

Windows:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 13. Running the Application

The pipeline is controlled through `run_pipeline.py`.

### Generate a Dataset

```bash
python run_pipeline.py --generate --size 100
```

### Run All Loading Strategies

```bash
python run_pipeline.py --strategy all --size 10k
```

### Run an Individual Strategy

```bash
python run_pipeline.py --strategy incremental --size 10k
```

Supported strategy values:

- `full_refresh`
- `incremental`
- `partition_replace`
- `all`

### Run a Benchmark

```bash
python run_pipeline.py --benchmark --size 100k --repeat 5
```

### Run a Localized Workload

```bash
python run_pipeline.py --benchmark --size 100k --hot-days 5
```

### Run the Streamlit Dashboard

```bash
streamlit run dashboard/app.py
```

### Run Automated Tests

```bash
python -m pytest -v
```

### Verify Dataset Generator

```bash
python test_generator.py
```

### CLI Configuration

The application uses YAML configuration files.

Default configuration:

`config/config.yaml`

Isolated validation configuration:

`config/config_validation.yaml`

To execute with the validation configuration:

```bash
python run_pipeline.py --config config/config_validation.yaml --strategy all --size 100
```

The validation configuration uses separate data and database paths. Keep it isolated from the original warehouse when testing changes.

**Important:** The `--reset` option rebuilds the configured warehouse. Use it only when you understand which database path is configured.

---

## 14. Testing and Verification

The project includes automated tests covering ingestion, validation, transformation, loading strategies, reconciliation, idempotency, and failure recovery.

### Test Execution

```bash
python -m pytest -v
```

### Latest Test Results

| Test Category | Result |
|---|---:|
| Idempotency and Recovery | 6 passed |
| Ingestion | 7 passed |
| End-to-End Pipeline | 6 passed |
| Reconciliation | 6 passed |
| Loading Strategies | 11 passed |
| Transformation | 8 passed |
| Validation | 16 passed |
| **Total** | **60 passed** |

Latest recorded execution:

```text
collected 60 items

60 passed in 6.22s
```

Additional generator verification:

```text
Batch 1 events: 204
Batch 2 events: 204
Batch 1 unique IDs: 200
Batch 2 unique IDs: 200
Cross-batch collisions: 0
Batch 1 duplicate deliveries: 4
Batch 2 duplicate deliveries: 4
```

The test suite uses an independent reference implementation to validate expected data states rather than relying exclusively on the loading strategy implementations.

---

## 15. Design Decisions and Trade-offs

| Design Decision | Reason |
|---|---|
| Three interchangeable loading strategies | Enables direct comparison using identical input |
| SQLite warehouse | Lightweight relational storage without external infrastructure |
| Integer paise for amounts | Avoids floating-point precision issues |
| SHA-256 batch identification | Supports deterministic batch tracking |
| Transactional processing | Prevents partial database updates |
| Event timestamp ordering | Handles out-of-order and late-arriving events |
| Logical date partitions | Demonstrates partition replacement without requiring a distributed database |
| Independent reconciliation | Verifies final data correctness |
| YAML configuration | Separates execution settings from application logic |

Additional design details are documented in `DESIGN_DECISIONS.md`.

Data fields, schemas, and rejection codes are documented in `DATA_DICTIONARY.md`.

---

## 16. Known Limitations

The implementation has a deliberately limited scope.

1. **SQLite partitioning:** Partition Replace uses logical date filters. It does not create physical database partitions.
2. **Cross-batch delete tombstones:** The current implementation does not persist deletion version metadata. A stale event arriving in a later batch for an order deleted in an earlier batch can be treated as an insert by Incremental Merge or Partition Replace. This is a known limitation.
3. **Single-process execution:** The framework is designed for local execution and does not implement distributed processing.
4. **Customer lifecycle:** Later customer deletions and their downstream referential effects are outside the current scope.
5. **Performance portability:** Benchmark timings depend on the machine, database, and workload distribution.
6. **Storage fallback:** When PyArrow is unavailable, the pipeline can fall back to CSV output where supported.

---

## 17. Future Improvements

Potential extensions include:

- Persistent delete tombstones across batches.
- Physical partitioning using a database designed for analytical workloads.
- Support for PostgreSQL or DuckDB.
- Distributed processing using Apache Spark.
- Incremental ingestion from streaming sources.
- Parallel strategy benchmarking.
- Expanded observability and execution dashboards.
- Automated benchmark comparison across different environments.

---

## 18. Conclusion

DataSync Benchmark demonstrates how different data loading strategies can be implemented and evaluated within a common data processing framework.

The project focuses on three important engineering requirements:

1. **Correctness:** Different loading strategies must produce equivalent analytical results.
2. **Reliability:** Transaction handling, idempotency, and recovery must prevent inconsistent data states.
3. **Performance:** Benchmarking must measure the trade-offs between full-table processing, incremental changes, and partition-level replacement.

The framework provides a reproducible foundation for understanding data loading strategies and their behaviour under different workloads.

---

**Project:** DataSync Benchmark  
**Language:** Python 3.12  
**Architecture:** Three-Tier Architecture  
**Database:** SQLite  
**Testing:** Pytest — 60 tests passed