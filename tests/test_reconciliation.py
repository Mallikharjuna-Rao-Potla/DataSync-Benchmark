import pandas as pd

from src.reconciliation.comparator import compare, reconcile
from src.storage.database import ORDER_COLS
from tests.helpers import BASELINE


def df():
    return pd.DataFrame(BASELINE, columns=ORDER_COLS)


def test_identical_passes():
    r = reconcile({"a": df(), "b": df()}, df())
    assert r["all_strategies_equivalent"] and r["overall"] == "ALL STRATEGIES EQUIVALENT"


def test_missing_record_detected():
    res = compare("x", df().iloc[:-1], df())
    assert res["status"] == "FAIL" and res["checks"]["missing_records"]["sample"] == ["O4"]
    assert not res["checks"]["row_count"]["pass"]


def test_extra_record_detected():
    t = pd.concat([df(), df().iloc[[0]].assign(order_id="O9")])
    assert compare("x", t, df())["checks"]["extra_records"]["sample"] == ["O9"]


def test_value_mismatch_detected_even_when_counts_match():
    t = df(); t.loc[1, "amount"] = 2001
    res = compare("x", t, df())
    assert res["checks"]["row_count"]["pass"] and not res["checks"]["value_mismatches"]["pass"]
    assert not res["checks"]["record_hash"]["pass"] and res["status"] == "FAIL"


def test_duplicate_primary_key_detected():
    t = pd.concat([df(), df().iloc[[0]]])
    assert not compare("x", t, df())["checks"]["primary_key_unique"]["pass"]


def test_cross_strategy_mismatch_flags_overall_failure():
    t = df(); t.loc[0, "status"] = "CANCELLED"
    r = reconcile({"a": df(), "b": t}, df())
    assert not r["all_strategies_equivalent"] and r["overall"] == "MISMATCH DETECTED"
