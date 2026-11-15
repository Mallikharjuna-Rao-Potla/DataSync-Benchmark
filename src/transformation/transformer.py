"""Deterministic transformations + the pure `apply_changes` state-transition function."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd

from src.storage.database import ORDER_COLS

CHANGE_COLS = ["event_id", "order_id", "operation", "customer_id", "order_date",
               "amount", "status", "updated_at", "source_sequence"]


# ---- parsers (shared with the validator so "valid" == "transformable") ------------------
def parse_dates(s):
    """YYYY-MM-DD or DD-MM-YYYY -> 'YYYY-MM-DD' (NaN if unparseable)."""
    s = s.astype("string").str.strip()
    a = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    b = pd.to_datetime(s, format="%d-%m-%Y", errors="coerce")
    return a.fillna(b).dt.strftime("%Y-%m-%d")


def parse_timestamps(s):
    """ISO-8601-ish -> 'YYYY-MM-DD HH:MM:SS' (NaN if unparseable)."""
    s = s.astype("string").str.strip()
    return pd.to_datetime(s, format="ISO8601", errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")


def _paise(v):
    if v is None or v is pd.NA or (isinstance(v, float) and v != v):
        return None
    try:
        d = Decimal(str(v).strip().replace(",", ""))
    except InvalidOperation:
        return None
    if not d.is_finite():
        return None
    return int((d * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def parse_amount_paise(s):
    return s.map(_paise).astype("Int64")


# ---- transforms --------------------------------------------------------------------------
def _clean(s, upper=False):
    s = s.astype("string").str.strip()
    return s.str.upper() if upper else s


def transform_orders(df):
    out = pd.DataFrame({
        "order_id": _clean(df["order_id"]),
        "customer_id": _clean(df["customer_id"]),
        "order_date": parse_dates(df["order_date"]),
        "amount": parse_amount_paise(df["amount"]),
        "status": _clean(df["status"], upper=True),
        "updated_at": parse_timestamps(df["updated_at"]),
    })
    return out[ORDER_COLS].astype({"amount": "int64"}).reset_index(drop=True)


def transform_events(df):
    is_delete = _clean(df["operation"], upper=True) == "DELETE"
    out = pd.DataFrame({
        "event_id": _clean(df["event_id"]),
        "order_id": _clean(df["order_id"]),
        "operation": _clean(df["operation"], upper=True),
        "customer_id": _clean(df["customer_id"]).mask(is_delete),
        "order_date": parse_dates(df["order_date"]).mask(is_delete),
        "amount": parse_amount_paise(df["amount"]).mask(is_delete),
        "status": _clean(df["status"], upper=True).mask(is_delete),
        "updated_at": parse_timestamps(df["updated_at"]),
        "source_sequence": pd.to_numeric(df["source_sequence"]).astype("int64"),
    })
    return out[CHANGE_COLS].reset_index(drop=True)


def collapse_events(events):
    """One net change per order: the event with the greatest (updated_at, source_sequence).
    This is how out-of-order delivery inside a batch is neutralised."""
    if len(events) == 0:
        return events.copy()
    ordered = events.sort_values(["updated_at", "source_sequence"], kind="mergesort")
    return ordered.drop_duplicates("order_id", keep="last").reset_index(drop=True)


def apply_changes(base, changes):
    """Apply net changes to a set of order rows. Pure function (used by Full Refresh,
    Partition Replacement and the reconciliation reference).

    Rule: a change older than the existing row's updated_at is STALE and ignored.
    Returns (new_df, stats)."""
    stats = dict(inserted=0, updated=0, deleted=0, skipped_stale=0, noop_deletes=0)
    base = base[ORDER_COLS]
    if len(changes) == 0:
        return base.sort_values("order_id").reset_index(drop=True), stats
    cur = base[["order_id", "updated_at"]].rename(columns={"updated_at": "cur_updated_at"})
    ch = changes.merge(cur, on="order_id", how="left")
    exists = ch["cur_updated_at"].notna()
    stale = exists & (ch["updated_at"] < ch["cur_updated_at"])
    active = ch[~stale]
    is_del = active["operation"] == "DELETE"
    ups = active[~is_del]
    dels = active[is_del]
    ups_exist = ups["cur_updated_at"].notna()
    stats.update(
        inserted=int((~ups_exist).sum()), updated=int(ups_exist.sum()),
        deleted=int(dels["cur_updated_at"].notna().sum()),
        noop_deletes=int(dels["cur_updated_at"].isna().sum()),
        skipped_stale=int(stale.sum()),
    )
    kept = base[~base["order_id"].isin(active["order_id"])]
    result = pd.concat([kept, ups[ORDER_COLS].astype({"amount": "int64"})], ignore_index=True)
    return result.sort_values("order_id").reset_index(drop=True), stats
