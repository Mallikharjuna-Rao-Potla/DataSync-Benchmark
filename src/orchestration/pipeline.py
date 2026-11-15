"""
Orchestrator: coordinates the complete data pipeline.

Pipeline flow:
    1. Ingest
    2. Validate
    3. Transform
    4. Load
    5. Reconcile
    6. Report

Architecture:
    Presentation Layer:
        CLI / Dashboard

    Application Layer:
        Pipeline orchestration
        Ingestion
        Validation
        Transformation
        Strategies
        Reconciliation
        Monitoring

    Data Layer:
        SQLite warehouse
        Staging
        Quarantine
        Reports

Business logic remains inside individual components.
This module coordinates their execution.
"""

import json
import time

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.config import resolve
from src.ingestion import data_generator
from src.ingestion.source_reader import (
    SourceError,
    file_hash,
    read_csv,
)

from src.monitoring.logger import get_logger
from src.monitoring.run_tracker import RunTracker, new_run_id

from src.reconciliation.comparator import reconcile

from src.storage import repositories as repo

from src.storage.database import (
    TARGETS,
    connect,
    init_db,
    read_table,
    transaction,
)

from src.strategies import get_strategy
from src.strategies.base_strategy import SourceData

from src.transformation.transformer import (
    apply_changes,
    collapse_events,
    transform_events,
    transform_orders,
)

from src.validation.schema_validator import SchemaError

from src.validation.validator import (
    validate_baseline,
    validate_events,
)


# =========================================================
# CONSTANTS
# =========================================================

ALL = [
    "full_refresh",
    "incremental",
    "partition_replace",
]


# =========================================================
# PREPARED BATCH
# =========================================================

@dataclass
class PreparedBatch:
    """
    Stores all data prepared for one incoming batch.

    Attributes
    ----------
    batch_id:
        Content-based identifier of the incoming file.

    source_file:
        Original source filename.

    input_records:
        Total number of input records.

    valid_events:
        Validated and transformed events.
        Duplicate event IDs are removed during transformation.

    changes:
        Net changes after collapsing events by order ID.

    rejected:
        Rejected incoming event records.

    customers:
        Validated customer records.

    baseline:
        Validated and transformed baseline orders.

    baseline_rejected:
        Rejected baseline records.
    """

    batch_id: str
    source_file: str
    input_records: int

    valid_events: pd.DataFrame
    changes: pd.DataFrame

    rejected: pd.DataFrame

    customers: pd.DataFrame
    baseline: pd.DataFrame
    baseline_rejected: pd.DataFrame


# =========================================================
# OUTPUT UTILITIES
# =========================================================

def write_frame(df, path_no_ext):
    """
    Write a DataFrame to Parquet.

    If PyArrow is unavailable, CSV is used as a fallback.

    Parameters
    ----------
    df:
        DataFrame to write.

    path_no_ext:
        Output path without file extension.

    Returns
    -------
    Path:
        Actual generated file path.
    """

    path = Path(path_no_ext)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        output_path = path.with_suffix(".parquet")

        df.to_parquet(
            output_path,
            index=False,
        )

        return output_path

    except ImportError:

        output_path = path.with_suffix(".csv")

        df.to_csv(
            output_path,
            index=False,
        )

        return output_path


# =========================================================
# BATCH PREPARATION
# =========================================================

def prepare_batch(
    cfg,
    baseline_dir=None,
    incoming_dir=None,
):
    """
    Execute the ingestion, validation and transformation stages.

    Steps
    -----
    1. Read customer records.
    2. Read baseline orders.
    3. Read incoming order events.
    4. Validate baseline records.
    5. Validate incoming events.
    6. Transform valid records.
    7. Collapse events into net order-level changes.

    Raises
    ------
    SourceError:
        When source files cannot be read.

    SchemaError:
        When required columns are missing.
    """

    # -----------------------------------------------------
    # 1. Resolve source directories
    # -----------------------------------------------------

    baseline_dir = Path(
        baseline_dir or resolve(cfg, "raw_baseline_dir")
    )

    incoming_dir = Path(
        incoming_dir or resolve(cfg, "raw_incoming_dir")
    )

    files = cfg["files"]
    schema = cfg["schema"]

    allowed_status = set(
        cfg["pipeline"]["allowed_status"]
    )

    # -----------------------------------------------------
    # 2. Read customer data
    # -----------------------------------------------------

    customers = read_csv(
        baseline_dir / files["customers"],
        schema["customers"]["required"],
    )

    # -----------------------------------------------------
    # 3. Read baseline orders
    # -----------------------------------------------------

    baseline_raw = read_csv(
        baseline_dir / files["baseline_orders"],
        schema["baseline_orders"]["required"],
    )

    # -----------------------------------------------------
    # 4. Read incoming events
    # -----------------------------------------------------

    event_path = (
        incoming_dir / files["order_events"]
    )

    events_raw = read_csv(
        event_path,
        schema["order_events"]["required"],
    )

    # -----------------------------------------------------
    # 5. Prepare valid customer IDs
    # -----------------------------------------------------

    customer_ids = set(
        customers["customer_id"]
        .dropna()
        .str.strip()
    )

    # -----------------------------------------------------
    # 6. Validate baseline orders
    # -----------------------------------------------------

    baseline_valid, baseline_rejected = validate_baseline(
        baseline_raw,
        customer_ids,
        allowed_status,
        files["baseline_orders"],
    )

    # -----------------------------------------------------
    # 7. Validate incoming events
    # -----------------------------------------------------

    events_valid, events_rejected = validate_events(
        events_raw,
        customer_ids,
        allowed_status,
        files["order_events"],
    )

    # -----------------------------------------------------
    # 8. Transform valid events
    # -----------------------------------------------------

    transformed_events = transform_events(
        events_valid
    )

    # -----------------------------------------------------
    # 9. Collapse events into net changes
    # -----------------------------------------------------

    changes = collapse_events(
        transformed_events
    )

    # -----------------------------------------------------
    # 10. Generate content-based batch ID
    # -----------------------------------------------------

    batch_id = file_hash(event_path)[:16]

    # -----------------------------------------------------
    # 11. Return prepared batch
    # -----------------------------------------------------

    return PreparedBatch(

        batch_id=batch_id,

        source_file=files["order_events"],

        input_records=len(events_raw),

        valid_events=transformed_events,

        changes=changes,

        rejected=events_rejected,

        customers=customers,

        baseline=transform_orders(baseline_valid),

        baseline_rejected=baseline_rejected,
    )


# =========================================================
# STRATEGY EXECUTION
# =========================================================

def run_strategy(
    conn,
    strategy,
    batch,
    run_id,
    tracker=None,
):
    """
    Execute one loading strategy inside an atomic transaction.

    Transaction includes:
        1. Saving accepted events.
        2. Saving rejected records.
        3. Executing the loading strategy.
        4. Recording the processed batch.

    If execution fails, all transaction changes are rolled back.

    Returns
    -------
    dict:
        status
        result
        duration
        error
    """

    tracker = tracker or RunTracker(conn)

    strategy_name = strategy.name

    # -----------------------------------------------------
    # 1. Start execution tracking
    # -----------------------------------------------------

    tracker.start(
        run_id,
        strategy_name,
        batch.batch_id,
        batch.input_records,
        len(batch.valid_events),
        len(batch.rejected),
    )

    # -----------------------------------------------------
    # 2. Check batch idempotency
    # -----------------------------------------------------

    if repo.is_processed(
        conn,
        batch.batch_id,
        strategy_name,
    ):

        tracker.finish(
            run_id,
            strategy_name,
            "SKIPPED_DUPLICATE_BATCH",
        )

        return {
            "status": "SKIPPED_DUPLICATE_BATCH",
            "result": None,
            "duration": 0.0,
            "error": None,
        }

    # -----------------------------------------------------
    # 3. Execute atomic transaction
    # -----------------------------------------------------

    try:

        with transaction(conn):

            # Save accepted events.
            repo.save_events(
                conn,
                batch.valid_events,
                run_id,
                batch.batch_id,
                batch.source_file,
            )

            # Combine event and baseline rejection records.
            rejected_records = pd.concat(
                [
                    batch.rejected,
                    batch.baseline_rejected,
                ],
                ignore_index=True,
            )

            # Save rejected records.
            repo.save_rejected(
                conn,
                rejected_records,
                run_id,
                batch.batch_id,
            )

            # Measure strategy execution time.
            start_time = time.perf_counter()

            # Execute selected loading strategy.
            result = strategy.execute(
                SourceData(
                    batch.changes,
                    run_id,
                    batch.batch_id,
                ),
                conn,
            )

            duration = (
                time.perf_counter() - start_time
            )

            # Record successful batch processing.
            repo.mark_processed(
                conn,
                batch.batch_id,
                strategy_name,
                run_id,
            )

        # Transaction committed successfully.

    except Exception as exc:

        # Transaction context handles rollback.
        tracker.finish(
            run_id,
            strategy_name,
            "FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )

        return {
            "status": "FAILED",
            "result": None,
            "duration": None,
            "error": str(exc),
        }

    # -----------------------------------------------------
    # 4. Record successful execution
    # -----------------------------------------------------

    tracker.finish(
        run_id,
        strategy_name,
        "SUCCESS",
        result,
        duration,
        valid=len(batch.valid_events),
    )

    return {
        "status": "SUCCESS",
        "result": result,
        "duration": duration,
        "error": None,
    }


# =========================================================
# EXPECTED STATE
# =========================================================

def expected_state(conn):
    """
    Calculate the reference final state.

    Expected state is reconstructed using:

        Baseline Orders
              +
        All Accepted Events
              |
              v
        Collapse Events
              |
              v
        Apply Changes
              |
              v
        Expected Final State

    This reference state is independent of the loading
    strategy implementations.
    """

    # Read accepted event history.
    history = read_table(
        conn,
        "order_events",
        order_by="event_id",
    )

    # Collapse events by order ID.
    collapsed_history = collapse_events(
        history
    )

    # Read original baseline.
    baseline = read_table(
        conn,
        "baseline_orders",
    )

    # Reconstruct expected final state.
    final_state, _ = apply_changes(
        baseline,
        collapsed_history,
    )

    return final_state


# =========================================================
# PIPELINE ORCHESTRATOR
# =========================================================

class Pipeline:
    """
    Main application-layer orchestrator.

    Coordinates:
        Dataset generation
        Ingestion
        Validation
        Transformation
        Strategy execution
        Reconciliation
        Reporting

    All configurable paths are obtained from the active
    configuration file.
    """

    # -----------------------------------------------------
    # INITIALIZATION
    # -----------------------------------------------------

    def __init__(
        self,
        cfg,
        log=None,
    ):

        self.cfg = cfg

        self.log = log or get_logger(
            log_file=resolve(
                cfg,
                "log_file",
            )
        )

        self.db_path = Path(
            resolve(
                cfg,
                "warehouse_db",
            )
        )

    # =====================================================
    # DATASET GENERATION
    # =====================================================

    def ensure_dataset(
        self,
        size=None,
        hot_days=None,
    ):
        """
        Generate synthetic source data when necessary.

        Generation happens when:
            1. A dataset size is explicitly requested.
            2. Requested size differs from metadata.
            3. Requested hot_days differs from metadata.
            4. Baseline orders file does not exist.

        Important:
            Metadata and generated CSV files are written
            only to directories defined in the active
            configuration.

        Returns
        -------
        bool:
            True  -> New dataset generated.
            False -> Existing dataset reused.
        """

        # -------------------------------------------------
        # 1. Resolve configured directories
        # -------------------------------------------------

        baseline_dir = Path(
            resolve(
                self.cfg,
                "raw_baseline_dir",
            )
        )

        incoming_dir = Path(
            resolve(
                self.cfg,
                "raw_incoming_dir",
            )
        )

        # The metadata directory is the parent of the
        # configured baseline directory.
        #
        # Example:
        # baseline_dir = data/validation/raw/baseline
        #
        # raw_dir = data/validation/raw

        raw_dir = baseline_dir.parent

        metadata_file = (
            raw_dir / "_dataset.json"
        )

        # -------------------------------------------------
        # 2. Read existing dataset metadata
        # -------------------------------------------------

        if metadata_file.exists():

            try:

                metadata = json.loads(
                    metadata_file.read_text(
                        encoding="utf-8"
                    )
                )

            except (
                json.JSONDecodeError,
                OSError,
            ) as exc:

                raise RuntimeError(
                    "Unable to read dataset metadata: "
                    f"{metadata_file}"
                ) from exc

        else:

            metadata = {}

        existing_size = metadata.get("size")

        existing_hot_days = metadata.get(
            "hot_days"
        )

        existing_configuration = (
            existing_size,
            existing_hot_days,
        )

        requested_configuration = (
            size,
            hot_days,
        )

        # -------------------------------------------------
        # 3. Check baseline file
        # -------------------------------------------------

        baseline_file = (
            baseline_dir
            / self.cfg["files"]["baseline_orders"]
        )

        # -------------------------------------------------
        # 4. Determine whether generation is required
        # -------------------------------------------------

        needs_generation = (

            size is not None

            and (

                existing_configuration
                != requested_configuration

                or not baseline_file.exists()

            )
        )

        # -------------------------------------------------
        # 5. Reuse existing dataset if possible
        # -------------------------------------------------

        if not needs_generation:

            self.log.info(
                "Dataset generation skipped. "
                "Existing dataset matches the requested "
                "configuration."
            )

            return False

        # -------------------------------------------------
        # 6. Prepare output directories
        # -------------------------------------------------

        raw_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        baseline_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        incoming_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        # -------------------------------------------------
        # 7. Read generator configuration
        # -------------------------------------------------

        generator_cfg = self.cfg["generator"]

        seed = generator_cfg["seed"]

        customers_ratio = generator_cfg[
            "customers_ratio"
        ]

        events_ratio = generator_cfg[
            "events_ratio"
        ]

        # -------------------------------------------------
        # 8. Log generation details
        # -------------------------------------------------

        self.log.info(
            f"Generating dataset: {size:,} orders "
            f"(seed={seed}, hot_days={hot_days})"
        )

        # -------------------------------------------------
        # 9. Generate synthetic data
        # -------------------------------------------------

        data_generator.generate(

            size=size,

            base_dir=raw_dir,

            seed=seed,

            customers_ratio=customers_ratio,

            events_ratio=events_ratio,

            baseline_dir=baseline_dir,

            incoming_dir=incoming_dir,

            files=self.cfg["files"],

            hot_days=hot_days,

        )

        # -------------------------------------------------
        # 10. Log successful generation
        # -------------------------------------------------

        self.log.info(
            "Dataset generated successfully."
        )

        self.log.info(
            f"Dataset metadata: {metadata_file}"
        )

        self.log.info(
            f"Baseline directory: {baseline_dir}"
        )

        self.log.info(
            f"Incoming directory: {incoming_dir}"
        )

        return True

    # =====================================================
    # MAIN PIPELINE EXECUTION
    # =====================================================

    def run(
        self,
        strategies=("all",),
        reset=False,
        size=None,
        hot_days=None,
    ):
        """
        Execute the complete data pipeline.

        Parameters
        ----------
        strategies:
            Selected strategies or all strategies.

        reset:
            Reset the configured warehouse database.

        size:
            Optional synthetic dataset size.

        hot_days:
            Optional localized change distribution.

        Returns
        -------
        dict:
            Pipeline execution summary.
        """

        # -------------------------------------------------
        # 1. Generate or reuse dataset
        # -------------------------------------------------

        regenerated = self.ensure_dataset(
            size=size,
            hot_days=hot_days,
        )

        # A regenerated dataset requires a fresh database
        # for a consistent baseline and event history.
        reset = reset or regenerated

        # -------------------------------------------------
        # 2. Reset only the configured database
        # -------------------------------------------------

        if reset and self.db_path.exists():

            self.log.warning(
                f"Resetting configured database: {self.db_path}"
            )

            self.db_path.unlink()

        # -------------------------------------------------
        # 3. Initialize database directory
        # -------------------------------------------------

        self.db_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # -------------------------------------------------
        # 4. Connect to SQLite warehouse
        # -------------------------------------------------

        conn = connect(
            self.db_path
        )

        try:

            init_db(conn)

            tracker = RunTracker(conn)

            run_id = new_run_id()

            # -------------------------------------------------
            # 5. Resolve selected strategies
            # -------------------------------------------------

            names = (
                ALL
                if "all" in strategies
                else list(strategies)
            )

            self.log.info(
                f"{run_id} | strategies={names}"
            )

            # -------------------------------------------------
            # 6. Prepare incoming batch
            # -------------------------------------------------

            try:

                batch = prepare_batch(
                    self.cfg
                )

            except (
                SourceError,
                SchemaError,
                FileNotFoundError,
            ) as exc:

                self.log.error(
                    "Batch-level failure. "
                    f"Nothing loaded: {exc}"
                )

                tracker.start(
                    run_id,
                    "ingestion",
                    None,
                    0,
                    0,
                    0,
                )

                tracker.finish(
                    run_id,
                    "ingestion",
                    "FAILED",
                    error=(
                        f"{type(exc).__name__}: {exc}"
                    ),
                )

                return {
                    "run_id": run_id,
                    "status": "FAILED",
                    "error": str(exc),
                    "results": {},
                }

            # -------------------------------------------------
            # 7. Load baseline if warehouse is empty
            # -------------------------------------------------

            baseline_count = conn.execute(
                "SELECT COUNT(*) FROM baseline_orders"
            ).fetchone()[0]

            if baseline_count == 0:

                self.log.info(
                    "Loading initial baseline data."
                )

                with transaction(conn):

                    repo.load_baseline(
                        conn,
                        batch.customers,
                        batch.baseline,
                    )

            # -------------------------------------------------
            # 8. Log batch statistics
            # -------------------------------------------------

            self.log.info(
                f"input={batch.input_records:,} "
                f"valid={len(batch.valid_events):,} "
                f"rejected={len(batch.rejected):,} "
                f"net_changes={len(batch.changes):,} "
                f"batch={batch.batch_id}"
            )

            # -------------------------------------------------
            # 9. Handle empty batch
            # -------------------------------------------------

            if batch.input_records == 0:

                self.log.info(
                    "Empty batch: controlled no-op."
                )

                return {
                    "run_id": run_id,
                    "status": "NO_OP",
                    "results": {},
                }

            # -------------------------------------------------
            # 10. Execute selected strategies
            # -------------------------------------------------

            results = {}

            for strategy_name in names:

                strategy = get_strategy(
                    strategy_name
                )

                output = run_strategy(
                    conn,
                    strategy,
                    batch,
                    run_id,
                    tracker,
                )

                results[strategy_name] = output

                result = output["result"]

                # ---------------------------------------------
                # Log strategy execution
                # ---------------------------------------------

                if result is not None:

                    self.log.info(

                        f"{strategy_name:<18} "
                        f"{output['status']:<24} "

                        f"ins={result.inserted} "
                        f"upd={result.updated} "
                        f"del={result.deleted} "
                        f"stale={result.skipped_stale} "

                        f"rows={result.final_row_count:,} "

                        f"time={output['duration']:.3f}s"

                    )

                else:

                    self.log.info(

                        f"{strategy_name:<18} "
                        f"{output['status']:<24} "

                        f"{output['error'] or ''}"

                    )

            # -------------------------------------------------
            # 11. Check strategy execution results
            # -------------------------------------------------

            successful_strategies = [

                name

                for name, output in results.items()

                if output["status"] in (
                    "SUCCESS",
                    "SKIPPED_DUPLICATE_BATCH",
                )

            ]

            # -------------------------------------------------
            # 12. Reconciliation
            # -------------------------------------------------

            reconciliation = None

            if len(successful_strategies) == len(names):

                reconciliation = (
                    self._reconcile_and_report(
                        conn,
                        names,
                        batch,
                        run_id,
                    )
                )

                self.log.info(
                    "Reconciliation: "
                    f"{reconciliation['overall']}"
                )

            # -------------------------------------------------
            # 13. Determine final pipeline status
            # -------------------------------------------------

            pipeline_status = (

                "SUCCESS"

                if (

                    len(successful_strategies)
                    == len(names)

                    and (

                        reconciliation is None

                        or reconciliation[
                            "all_strategies_equivalent"
                        ]

                    )

                )

                else "FAILED"

            )

            # -------------------------------------------------
            # 14. Return pipeline summary
            # -------------------------------------------------

            return {

                "run_id": run_id,

                "status": pipeline_status,

                "results": results,

                "reconciliation": reconciliation,

                "batch": batch,

            }

        finally:

            # Always close the database connection.
            conn.close()

    # =====================================================
    # RECONCILIATION AND REPORTING
    # =====================================================

    def _reconcile_and_report(
        self,
        conn,
        names,
        batch,
        run_id,
    ):
        """
        Compare strategy outputs against the expected state.

        Reports generated:
            1. reconciliation_report.json
            2. data_quality_report.json
            3. performance_report.csv
            4. valid_events.parquet / CSV
            5. transformed_orders.parquet / CSV
            6. rejected_records.parquet / CSV
        """

        # -------------------------------------------------
        # 1. Reconstruct expected state
        # -------------------------------------------------

        expected = expected_state(
            conn
        )

        # -------------------------------------------------
        # 2. Save expected state to staging
        # -------------------------------------------------

        with transaction(conn):

            repo.replace_staging(
                conn,
                expected,
            )

        # -------------------------------------------------
        # 3. Read target tables
        # -------------------------------------------------

        targets = {

            name: read_table(
                conn,
                TARGETS[name],
            )

            for name in names

        }

        # -------------------------------------------------
        # 4. Reconcile all selected strategies
        # -------------------------------------------------

        reconciliation = reconcile(
            targets,
            expected,
        )

        # -------------------------------------------------
        # 5. Resolve reports directory
        # -------------------------------------------------

        reports_dir = Path(
            resolve(
                self.cfg,
                "reports_dir",
            )
        )

        reports_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        # -------------------------------------------------
        # 6. Generate reconciliation report
        # -------------------------------------------------

        reconciliation_file = (
            reports_dir / "reconciliation_report.json"
        )

        reconciliation_file.write_text(

            json.dumps(
                reconciliation,
                indent=2,
                default=str,
            ),

            encoding="utf-8",

        )

        # -------------------------------------------------
        # 7. Generate data quality report
        # -------------------------------------------------

        if len(batch.rejected):

            rejected_reasons = (
                batch.rejected["reject_reason"]
                .value_counts()
                .to_dict()
            )

        else:

            rejected_reasons = {}

        data_quality_report = {

            "run_id": run_id,

            "batch_id": batch.batch_id,

            "input_records": batch.input_records,

            "valid_records": len(
                batch.valid_events
            ),

            "rejected_records": len(
                batch.rejected
            ),

            "rejected_by_reason": rejected_reasons,

            "net_changes_after_collapse": len(
                batch.changes
            ),

            "baseline_rows": len(
                batch.baseline
            ),

            "baseline_rejected": len(
                batch.baseline_rejected
            ),

        }

        data_quality_file = (
            reports_dir / "data_quality_report.json"
        )

        data_quality_file.write_text(

            json.dumps(
                data_quality_report,
                indent=2,
            ),

            encoding="utf-8",

        )

        # -------------------------------------------------
        # 8. Generate performance report
        # -------------------------------------------------

        runs = pd.read_sql_query(

            "SELECT * FROM pipeline_runs WHERE run_id=?",

            conn,

            params=(run_id,),

        )

        performance_file = (
            reports_dir / "performance_report.csv"
        )

        runs.to_csv(
            performance_file,
            index=False,
        )

        # -------------------------------------------------
        # 9. Resolve staging and quarantine directories
        # -------------------------------------------------

        staging_dir = Path(
            resolve(
                self.cfg,
                "staging_dir",
            )
        )

        quarantine_dir = Path(
            resolve(
                self.cfg,
                "quarantine_dir",
            )
        )

        # -------------------------------------------------
        # 10. Write valid events
        # -------------------------------------------------

        write_frame(
            batch.valid_events,
            staging_dir / "valid_events",
        )

        # -------------------------------------------------
        # 11. Write expected transformed orders
        # -------------------------------------------------

        write_frame(
            expected,
            staging_dir / "transformed_orders",
        )

        # -------------------------------------------------
        # 12. Write rejected records
        # -------------------------------------------------

        rejected_records = pd.concat(

            [
                batch.rejected,
                batch.baseline_rejected,
            ],

            ignore_index=True,

        )

        write_frame(
            rejected_records,
            quarantine_dir / "rejected_records",
        )

        # -------------------------------------------------
        # 13. Log report locations
        # -------------------------------------------------

        self.log.info(
            "Reports generated successfully."
        )

        self.log.info(
            f"Reports directory: {reports_dir}"
        )

        # -------------------------------------------------
        # 14. Return reconciliation result
        # -------------------------------------------------

        return reconciliation