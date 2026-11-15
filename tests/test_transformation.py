import pandas as pd

from src.transformation.transformer import (apply_changes, collapse_events, parse_amount_paise,
                                            parse_dates, parse_timestamps, transform_events)
from tests.helpers import BASELINE, SCENARIO, events_df, oracle
from src.storage.database import ORDER_COLS


def s(*v):
    return pd.Series(list(v), dtype="string")


def test_date_normalisation():
    assert parse_dates(s("15-11-2026", "2026-11-15", "junk")).tolist()[:2] == ["2026-11-15"] * 2
    assert pd.isna(parse_dates(s("junk")).iloc[0])


def test_amount_to_integer_paise():
    assert parse_amount_paise(s("1500")).tolist() == [150000]
    assert parse_amount_paise(s("1,234.565", "0.1", "abc")).tolist()[:2] == [123457, 10]   # half-up
    assert pd.isna(parse_amount_paise(s("abc")).iloc[0])


def test_timestamp_normalisation():
    assert parse_timestamps(s("2026-02-01T10:00:00")).iloc[0] == "2026-02-01 10:00:00"


def test_transform_is_deterministic():
    raw = pd.DataFrame([dict(event_id="E1", order_id=" O1 ", operation="update", customer_id="C1",
                             order_date="15-11-2026", amount="1500", status="paid",
                             updated_at="2026-02-01 10:00:00", source_sequence="3")])
    a, b = transform_events(raw), transform_events(raw)
    pd.testing.assert_frame_equal(a, b)
    r = a.iloc[0]
    assert (r.order_id, r.operation, r.order_date, r.amount, r.status) == ("O1", "UPDATE", "2026-11-15", 150000, "PAID")


def test_delete_events_have_null_business_fields():
    raw = pd.DataFrame([dict(event_id="E1", order_id="O1", operation="DELETE", customer_id="C1",
                             order_date="2026-01-01", amount="5", status="PAID",
                             updated_at="2026-02-01 10:00:00", source_sequence="1")])
    assert transform_events(raw).iloc[0][["customer_id", "order_date", "amount", "status"]].isna().all()


def test_collapse_picks_latest_timestamp_not_latest_row():
    rows = [("UPDATE", "O1", "C1", "2026-01-01", 1, "PAID", "2026-02-02 00:00:00"),
            ("UPDATE", "O1", "C1", "2026-01-01", 2, "PAID", "2026-02-01 00:00:00")]   # arrives later, is older
    c = collapse_events(events_df(rows))
    assert len(c) == 1 and c.iloc[0]["amount"] == 1


def test_collapse_tie_broken_by_sequence():
    rows = [("UPDATE", "O1", "C1", "2026-01-01", 1, "PAID", "2026-02-01 00:00:00"),
            ("UPDATE", "O1", "C1", "2026-01-01", 2, "PAID", "2026-02-01 00:00:00")]
    assert collapse_events(events_df(rows)).iloc[0]["amount"] == 2


def test_apply_changes_matches_oracle_and_stats():
    base = pd.DataFrame(BASELINE, columns=ORDER_COLS)
    out, st = apply_changes(base, collapse_events(events_df(SCENARIO)))
    exp = oracle(BASELINE, SCENARIO)
    assert [tuple(r) for r in out.itertuples(index=False, name=None)] == [
        tuple(int(x) if i == 3 else x for i, x in enumerate(r)) for r in exp]
    assert st == dict(inserted=1, updated=1, deleted=2, skipped_stale=1, noop_deletes=1)


def test_insert_of_existing_order_acts_as_update():
    base = pd.DataFrame(BASELINE, columns=ORDER_COLS)
    ch = collapse_events(events_df([("INSERT", "O1", "C1", "2026-01-01", 77, "PAID", "2026-03-01 00:00:00")]))
    out, st = apply_changes(base, ch)
    assert st["updated"] == 1 and st["inserted"] == 0 and len(out) == 4
