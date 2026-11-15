from types import SimpleNamespace

import pandas as pd

from src.storage import repositories as repo
from src.storage.database import ORDER_COLS, TARGETS, connect, init_db, read_table, transaction
from src.strategies import get_strategy
from src.transformation.transformer import CHANGE_COLS, collapse_events

BASELINE = [
    ("O1", "C1", "2026-01-01", 1000, "PLACED", "2026-01-01 10:00:00"),
    ("O2", "C1", "2026-01-01", 2000, "PAID", "2026-01-01 11:00:00"),
    ("O3", "C2", "2026-01-02", 3000, "PAID", "2026-01-02 09:00:00"),
    ("O4", "C3", "2026-01-03", 4000, "SHIPPED", "2026-01-03 09:00:00"),
]
# (operation, order_id, customer, date, amount, status, updated_at)
SCENARIO = [
    ("UPDATE", "O1", "C1", "2026-01-05", 1500, "PAID", "2026-02-01 10:00:00"),    # moves partition
    ("DELETE", "O2", None, None, None, None, "2026-02-01 10:01:00"),
    ("INSERT", "O5", "C2", "2026-01-02", 5000, "PLACED", "2026-02-01 10:02:00"),
    ("UPDATE", "O3", "C2", "2026-01-02", 9999, "PAID", "2026-01-01 00:00:00"),    # stale (late-arriving)
    ("DELETE", "O99", None, None, None, None, "2026-02-01 10:03:00"),             # delete of unknown order
    ("DELETE", "O4", None, None, None, None, "2026-02-01 10:04:00"),              # empties partition 2026-01-03
]


def make_db(baseline=BASELINE):
    conn = connect(":memory:")
    init_db(conn)
    customers = pd.DataFrame({"customer_id": [f"C{i}" for i in range(1, 6)],
                              "customer_name": "x", "created_at": "2025-01-01 00:00:00"})
    with transaction(conn):
        repo.load_baseline(conn, customers, pd.DataFrame(baseline, columns=ORDER_COLS))
    return conn


def events_df(rows, tag="b1"):
    recs = [dict(event_id=f"{tag}-{i}", order_id=r[1], operation=r[0], customer_id=r[2],
                 order_date=r[3], amount=r[4], status=r[5], updated_at=r[6], source_sequence=i + 1)
            for i, r in enumerate(rows)]
    df = pd.DataFrame(recs, columns=CHANGE_COLS)
    df["amount"] = df["amount"].astype("Int64")
    return df


def make_batch(rows, batch_id="b1"):
    ev = events_df(rows, batch_id)
    return SimpleNamespace(batch_id=batch_id, source_file="t.csv", input_records=len(ev),
                           valid_events=ev, changes=collapse_events(ev),
                           rejected=pd.DataFrame(), baseline_rejected=pd.DataFrame())


def oracle(baseline, change_rows):
    """Independent, deliberately naive reference: replay events one by one in
    (updated_at, sequence) order against a dict. Different algorithm from collapse+merge."""
    state = {r[0]: dict(zip(ORDER_COLS, r)) for r in baseline}
    evs = sorted(enumerate(change_rows), key=lambda x: (x[1][6], x[0]))
    for _, (op, oid, cust, date, amt, status, ts) in evs:
        cur = state.get(oid)
        if cur is not None and ts < cur["updated_at"]:
            continue
        if op == "DELETE":
            state.pop(oid, None)
        else:
            state[oid] = dict(order_id=oid, customer_id=cust, order_date=date, amount=amt,
                              status=status, updated_at=ts)
    return sorted(tuple(v[c] for c in ORDER_COLS) for v in state.values())


def target_rows(conn, name):
    df = read_table(conn, TARGETS[name])
    return sorted(tuple(int(v) if c == "amount" else v for c, v in zip(ORDER_COLS, r))
                  for r in df[ORDER_COLS].itertuples(index=False, name=None))


def run(conn, name, batch, run_id="r1"):
    from src.orchestration.pipeline import run_strategy
    return run_strategy(conn, get_strategy(name), batch, run_id)
