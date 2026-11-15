"""Record-level validation. Bad records are quarantined (never silently dropped)."""
import json

import pandas as pd

from src.transformation.transformer import (parse_amount_paise, parse_dates, parse_timestamps)

VALID_OPS = {"INSERT", "UPDATE", "DELETE"}
REJECT_COLS = ["source_file", "record_type", "source_row", "record_key", "reject_reason", "raw_record"]


def _flag(reason, mask, code):
    reason.loc[mask.fillna(False).to_numpy(dtype=bool) & reason.isna().to_numpy()] = code


def _check_order_fields(df, reason, needs, customers, allowed_status):
    """Field checks for rows where `needs` is True (INSERT/UPDATE/baseline rows)."""
    s = lambda c: df[c].astype("string").str.strip()          # noqa: E731
    for col in ("customer_id", "order_date", "amount", "status"):
        _flag(reason, needs & s(col).isna(), "MISSING_REQUIRED_FIELD")
    dates = parse_dates(df["order_date"])
    _flag(reason, needs & dates.isna(), "INVALID_DATE")
    paise = parse_amount_paise(df["amount"])
    _flag(reason, needs & paise.isna(), "INVALID_AMOUNT")
    _flag(reason, needs & (paise < 0), "NEGATIVE_AMOUNT")
    _flag(reason, needs & ~s("customer_id").isin(customers), "INVALID_CUSTOMER")
    _flag(reason, needs & ~s("status").str.upper().isin(allowed_status), "INVALID_STATUS")


def _quarantine(df, reason, source_file, record_type, key_col):
    bad = df[reason.notna().to_numpy()]
    if len(bad) == 0:
        return pd.DataFrame(columns=REJECT_COLS)
    raw = [json.dumps({k: (None if pd.isna(v) else v) for k, v in r.items()})
           for r in bad.to_dict("records")]
    return pd.DataFrame({
        "source_file": source_file, "record_type": record_type,
        "source_row": (bad.index + 2).astype(int),        # +2 = header + 1-based
        "record_key": bad[key_col].astype("string").to_numpy(),
        "reject_reason": reason[reason.notna()].to_numpy(), "raw_record": raw,
    })


def validate_events(df, customers, allowed_status, source_file="order_events.csv"):
    """Returns (valid_df, rejected_df). Order of checks defines the reject_reason."""
    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    s = lambda c: df[c].astype("string").str.strip()          # noqa: E731
    op = s("operation").str.upper()
    _flag(reason, s("order_id").isna(), "MISSING_ORDER_ID")
    _flag(reason, s("event_id").isna(), "MISSING_EVENT_ID")
    _flag(reason, ~op.isin(VALID_OPS), "INVALID_OPERATION")
    _flag(reason, parse_timestamps(df["updated_at"]).isna(), "INVALID_TIMESTAMP")
    _flag(reason, pd.to_numeric(df["source_sequence"], errors="coerce").isna(), "INVALID_SEQUENCE")
    _check_order_fields(df, reason, op.isin({"INSERT", "UPDATE"}), customers, allowed_status)
    # duplicates among otherwise-valid rows: first delivery wins, re-deliveries are quarantined
    ok = reason.isna()
    dup = ok & s("event_id").duplicated(keep="first")
    _flag(reason, dup, "DUPLICATE_EVENT")
    rejected = _quarantine(df, reason, source_file, "event", "event_id")
    return df[reason.isna().to_numpy()].copy(), rejected


def validate_baseline(df, customers, allowed_status, source_file="orders.csv"):
    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    s = lambda c: df[c].astype("string").str.strip()          # noqa: E731
    _flag(reason, s("order_id").isna(), "MISSING_ORDER_ID")
    _flag(reason, parse_timestamps(df["updated_at"]).isna(), "INVALID_TIMESTAMP")
    _check_order_fields(df, reason, pd.Series(True, index=df.index), customers, allowed_status)
    _flag(reason, reason.isna() & s("order_id").duplicated(keep="first"), "DUPLICATE_ORDER_ID")
    rejected = _quarantine(df, reason, source_file, "baseline", "order_id")
    return df[reason.isna().to_numpy()].copy(), rejected
