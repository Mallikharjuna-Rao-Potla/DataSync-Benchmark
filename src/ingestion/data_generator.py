"""
Deterministic synthetic e-commerce data generator.

Features:
    - Reproducible data using a fixed random seed.
    - Globally unique event IDs across independent batches.
    - Duplicate event delivery within a batch.
    - INSERT, UPDATE and DELETE operations.
    - Date changes between logical partitions.
    - Multiple date formats.
    - Lowercase statuses.
    - Out-of-order and stale events.
    - Invalid records for quarantine testing.

Amounts are generated as rupees in text format.
The transformer converts them into integer paise.
"""

import json
import re
import hashlib

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------

STATUSES = [
    "PLACED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]

BASE_DAY = datetime(2026, 1, 1)
EVENT_START = datetime(2026, 4, 15)

FMT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------
# MONEY GENERATOR
# ---------------------------------------------------------

def _money(rng, n):
    """Generate monetary values as strings in rupees."""

    values = rng.integers(10_000, 5_000_000, n)

    return [
        f"{value / 100:.2f}"
        for value in values
    ]


# ---------------------------------------------------------
# EVENT ID GENERATOR
# ---------------------------------------------------------

def _generate_event_ids(batch_id, count):
    """
    Generate namespaced event IDs.

    Same batch_id produces identical IDs.
    Different batch IDs produce different IDs.
    """

    prefix = re.sub(
        r"[^A-Za-z0-9_-]",
        "_",
        str(batch_id)
    )

    return [
        f"{prefix}_E{i:08d}"
        for i in range(1, count + 1)
    ]

# ---------------------------------------------------------
# MAIN DATA GENERATOR
# ---------------------------------------------------------

def generate(
    size,
    base_dir,
    cfg=None,
    seed=42,
    customers_ratio=0.1,
    events_ratio=0.2,
    baseline_dir=None,
    incoming_dir=None,
    files=None,
    hot_days=None,
    batch_id=None,
):
    """
    Generate synthetic e-commerce data.

    Parameters
    ----------
    size : int
        Number of baseline orders.

    base_dir : str or Path
        Main dataset directory.

    cfg : optional
        Reserved for compatibility with existing configuration.

    seed : int
        Random seed for reproducibility.

    customers_ratio : float
        Customer count relative to baseline order count.

    events_ratio : float
        Event count relative to baseline order count.

    baseline_dir : str or Path
        Directory for baseline CSV files.

    incoming_dir : str or Path
        Directory for incoming event CSV.

    files : dict
        Optional custom filenames.

    hot_days : int or None
        Concentrate changes on recent baseline dates.

    batch_id : str or None
        Unique identifier for an independently generated batch.

        IMPORTANT:
        Reuse the same batch_id when regenerating the same batch.
        Use a different batch_id for every independent batch.

        If omitted, a UUID is generated. This ensures uniqueness,
        but event IDs will differ between separate generator runs.

    Returns
    -------
    dict
        Dataset metadata.
    """

    # -----------------------------------------------------
    # 1. INPUT VALIDATION
    # -----------------------------------------------------

    if size <= 0:
        raise ValueError("size must be greater than zero.")

    if not 0 < customers_ratio <= 1:
        raise ValueError("customers_ratio must be between 0 and 1.")

    if not 0 < events_ratio <= 1:
        raise ValueError("events_ratio must be between 0 and 1.")

    if hot_days is not None and not 1 <= hot_days <= 90:
        raise ValueError("hot_days must be between 1 and 90.")

    # -----------------------------------------------------
    # 2. BATCH IDENTIFICATION
    # -----------------------------------------------------

    if batch_id is None:

        # Deterministic namespace derived from generator configuration.
        # Identical configurations produce identical event IDs.

        identity = {
            "seed": int(seed),
            "size": int(size),
            "customers_ratio": float(customers_ratio),
            "events_ratio": float(events_ratio),
            "hot_days": hot_days,
        }

        identity_string = json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":")
        )

        batch_id = hashlib.sha256(
            identity_string.encode("utf-8")
        ).hexdigest()[:32]

    batch_id = str(batch_id).strip()

    if not batch_id:
        raise ValueError("batch_id cannot be empty.")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError(
            "batch_id may contain only letters, numbers, "
            "underscores and hyphens."
        )

    # -----------------------------------------------------
    # 3. DIRECTORY CONFIGURATION
    # -----------------------------------------------------

    base_dir = Path(base_dir)

    baseline_dir = Path(
        baseline_dir or base_dir / "baseline"
    )

    incoming_dir = Path(
        incoming_dir or base_dir / "incoming"
    )

    baseline_dir.mkdir(parents=True, exist_ok=True)
    incoming_dir.mkdir(parents=True, exist_ok=True)

    files = files or {
        "customers": "customers.csv",
        "baseline_orders": "orders.csv",
        "order_events": "order_events.csv",
    }

    required_file_keys = {
        "customers",
        "baseline_orders",
        "order_events",
    }

    if not required_file_keys.issubset(files):
        raise ValueError(
            f"files must contain: {required_file_keys}"
        )

    # -----------------------------------------------------
    # 4. RANDOM NUMBER GENERATOR
    # -----------------------------------------------------

    rng = np.random.default_rng(seed)

    # -----------------------------------------------------
    # 5. CUSTOMER GENERATION
    # -----------------------------------------------------

    n_cust = max(
        10,
        int(size * customers_ratio)
    )

    cust_ids = [
        f"C{i:06d}"
        for i in range(1, n_cust + 1)
    ]

    customers = pd.DataFrame({
        "customer_id": cust_ids,

        "customer_name": [
            f"Customer {i}"
            for i in range(1, n_cust + 1)
        ],

        "created_at": "2025-12-01 00:00:00",
    })

    # -----------------------------------------------------
    # 6. BASELINE ORDER GENERATION
    # -----------------------------------------------------

    day_off = rng.integers(0, 90, size)

    secs = rng.integers(0, 86_400, size)

    dates = [
        BASE_DAY + timedelta(days=int(d))
        for d in day_off
    ]

    orders = pd.DataFrame({
        "order_id": [
            f"O{i:07d}"
            for i in range(1, size + 1)
        ],

        "customer_id": rng.choice(
            cust_ids,
            size
        ),

        "order_date": [
            d.strftime("%Y-%m-%d")
            for d in dates
        ],

        "amount": _money(rng, size),

        "status": rng.choice(
            STATUSES,
            size
        ),

        "updated_at": [
            (d + timedelta(seconds=int(s))).strftime(FMT)
            for d, s in zip(dates, secs)
        ],
    })

    base_updated = orders["updated_at"].tolist()

    oid_arr = orders["order_id"].tolist()

    odate_arr = orders["order_date"].tolist()

    # -----------------------------------------------------
    # 7. CHANGE EVENT CONFIGURATION
    # -----------------------------------------------------

    n_ev = max(
        10,
        int(size * events_ratio)
    )

    n_bad = max(
        2,
        int(n_ev * 0.04)
    )

    n_good = n_ev - n_bad

    ops = rng.choice(
        ["INSERT", "UPDATE", "DELETE"],
        n_good,
        p=[0.25, 0.60, 0.15]
    )

    # Select baseline orders affected by events.

    if hot_days is not None:

        eligible = np.flatnonzero(
            day_off >= 90 - hot_days
        )

        targets = rng.choice(
            eligible,
            n_good
        )

    else:

        targets = rng.integers(
            0,
            size,
            n_good
        )

    ev = []

    next_new = size + 1

    # Select late-arriving events.

    late_stale = set(
        rng.choice(
            n_good,
            max(1, int(n_good * 0.02)),
            replace=False
        ).tolist()
    )

    late_batch = set(
        rng.choice(
            n_good,
            max(1, int(n_good * 0.03)),
            replace=False
        ).tolist()
    )

    # -----------------------------------------------------
    # 8. DATE FORMAT GENERATOR
    # -----------------------------------------------------

    def alt_date(d):
        """
        Generate mixed date formats.

        Approximately 10% use DD-MM-YYYY.
        """

        if rng.random() < 0.10:
            return d.strftime("%d-%m-%Y")

        return d.strftime("%Y-%m-%d")

    # -----------------------------------------------------
    # 9. VALID EVENT GENERATION
    # -----------------------------------------------------

    for i in range(n_good):

        ts = EVENT_START + timedelta(
            seconds=i * 7
        )

        op = ops[i]

        row = int(targets[i])

        rec = {
            "operation": op,
            "order_id": oid_arr[row],
            "customer_id": None,
            "order_date": None,
            "amount": None,
            "status": None,
        }

        # INSERT operation.

        if op == "INSERT":

            rec["order_id"] = (
                f"O{next_new:07d}"
            )

            next_new += 1

        # INSERT and UPDATE contain order details.

        if op in ("INSERT", "UPDATE"):

            if hot_days is not None:

                date_offset = int(
                    rng.integers(
                        90 - hot_days,
                        90
                    )
                )

            else:

                date_offset = int(
                    rng.integers(0, 120)
                )

            d_new = BASE_DAY + timedelta(
                days=date_offset
            )

            # 80% of updates retain their original date.

            if op == "UPDATE" and rng.random() > 0.2:

                d_new = datetime.strptime(
                    odate_arr[row],
                    "%Y-%m-%d"
                )

            rec.update({
                "customer_id": cust_ids[
                    int(rng.integers(n_cust))
                ],

                "order_date": alt_date(d_new),

                "amount": _money(rng, 1)[0],

                "status": STATUSES[
                    int(rng.integers(len(STATUSES)))
                ],
            })

            # Inject lowercase statuses.

            if rng.random() < 0.05:

                rec["status"] = rec["status"].lower()

        # Inject stale events.

        if i in late_stale and op != "INSERT":

            ts = datetime.strptime(
                base_updated[row],
                FMT
            ) - timedelta(
                hours=int(rng.integers(1, 48))
            )

        # Inject out-of-order events.

        elif i in late_batch:

            ts = ts - timedelta(
                days=int(rng.integers(2, 10))
            )

        rec["updated_at"] = ts.strftime(FMT)

        ev.append(rec)

    # -----------------------------------------------------
    # 10. INVALID EVENT GENERATION
    # -----------------------------------------------------

    bad_kinds = [
        "neg",
        "null_id",
        "bad_cust",
        "bad_op",
        "bad_date",
        "no_amount",
    ]

    for k in range(n_bad):

        kind = bad_kinds[k % len(bad_kinds)]

        rec = {
            "operation": "INSERT",
            "order_id": f"O{next_new + k:07d}",
            "customer_id": cust_ids[0],
            "order_date": "2026-02-10",
            "amount": "250.00",
            "status": "PLACED",
            "updated_at": (
                EVENT_START + timedelta(
                    hours=1,
                    seconds=k
                )
            ).strftime(FMT),
        }

        if kind == "neg":

            rec["amount"] = "-500.00"

        elif kind == "null_id":

            rec["order_id"] = None

        elif kind == "bad_cust":

            rec["customer_id"] = "C999999"

        elif kind == "bad_op":

            rec["operation"] = "UPSERT"

        elif kind == "bad_date":

            rec["order_date"] = "not-a-date"

        elif kind == "no_amount":

            rec["amount"] = None

        ev.append(rec)

    # -----------------------------------------------------
    # 11. EVENT ID GENERATION
    # -----------------------------------------------------

    events = pd.DataFrame(ev)

    events.insert(
        0,
        "event_id",
        _generate_event_ids(
            batch_id,
            len(events)
        )
    )

    # -----------------------------------------------------
    # 12. DUPLICATE EVENT DELIVERY
    # -----------------------------------------------------

    # Simulate approximately 2% redelivery.
    # A duplicate delivery retains the original event_id.

    n_dup = max(
        1,
        int(len(events) * 0.02)
    )

    duplicate_indices = rng.choice(
        len(events),
        n_dup,
        replace=False
    )

    dup = events.iloc[
        duplicate_indices
    ].copy()

    events = pd.concat(
        [events, dup],
        ignore_index=True
    )

    # Every delivery gets a distinct source sequence.
    # Duplicate event IDs are subsequently quarantined
    # by validate_events(), keeping the first delivery.

    events["source_sequence"] = range(
        1,
        len(events) + 1
    )

    events = events[
        [
            "event_id",
            "order_id",
            "operation",
            "customer_id",
            "order_date",
            "amount",
            "status",
            "updated_at",
            "source_sequence",
        ]
    ]

    # -----------------------------------------------------
    # 13. WRITE CSV FILES
    # -----------------------------------------------------

    customers.to_csv(
        baseline_dir / files["customers"],
        index=False
    )

    orders.to_csv(
        baseline_dir / files["baseline_orders"],
        index=False
    )

    events.to_csv(
        incoming_dir / files["order_events"],
        index=False
    )
    # -----------------------------------------------------
    # 14. DATASET METADATA
    # -----------------------------------------------------
    meta = {
        "size": int(size),
        "seed": int(seed),
        "batch_id": batch_id,
        "hot_days": hot_days,
        "customers": int(n_cust),
        "events": int(len(events)),
        "unique_event_ids": int(events["event_id"].nunique()),
        "duplicate_deliveries": int(
            len(events) - events["event_id"].nunique()
        ),
    }

    (base_dir / "_dataset.json").write_text(
        json.dumps(meta, indent=4),
        encoding="utf-8"
    )
    return meta