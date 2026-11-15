import pandas as pd

from src.validation.validator import validate_baseline, validate_events

CUST = {"C1", "C2"}
ALLOWED = {"PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"}


def ev(**kw):
    base = dict(event_id="E1", order_id="O1", operation="INSERT", customer_id="C1",
                order_date="2026-01-01", amount="100", status="PLACED",
                updated_at="2026-02-01 10:00:00", source_sequence="1")
    base.update(kw)
    return base


def check(records):
    df = pd.DataFrame(records)
    return validate_events(df, CUST, ALLOWED)


def reasons(records):
    return check(records)[1]["reject_reason"].tolist()


def test_valid_event_passes():
    ok, rej = check([ev()])
    assert len(ok) == 1 and len(rej) == 0


def test_null_order_id():            assert reasons([ev(order_id=None)]) == ["MISSING_ORDER_ID"]
def test_negative_amount():          assert reasons([ev(amount="-500")]) == ["NEGATIVE_AMOUNT"]
def test_non_numeric_amount():       assert reasons([ev(amount="abc")]) == ["INVALID_AMOUNT"]
def test_missing_amount_on_insert(): assert reasons([ev(amount=None)]) == ["MISSING_REQUIRED_FIELD"]
def test_invalid_customer():         assert reasons([ev(customer_id="C9")]) == ["INVALID_CUSTOMER"]
def test_invalid_operation():        assert reasons([ev(operation="UPSERT")]) == ["INVALID_OPERATION"]
def test_invalid_date():             assert reasons([ev(order_date="31-31-2026")]) == ["INVALID_DATE"]
def test_invalid_timestamp():        assert reasons([ev(updated_at="yesterday")]) == ["INVALID_TIMESTAMP"]
def test_invalid_status():           assert reasons([ev(status="LOST")]) == ["INVALID_STATUS"]


def test_delete_needs_only_key_and_metadata():
    ok, rej = check([ev(operation="DELETE", customer_id=None, order_date=None, amount=None, status=None)])
    assert len(ok) == 1 and len(rej) == 0


def test_lowercase_operation_and_status_are_accepted():
    ok, _ = check([ev(operation="update", status=" paid ")])
    assert len(ok) == 1


def test_duplicate_event_first_wins_and_rest_quarantined():
    ok, rej = check([ev(), ev(order_id="O2")])      # same event_id E1 twice
    assert len(ok) == 1 and ok.iloc[0]["order_id"] == "O1"
    assert rej["reject_reason"].tolist() == ["DUPLICATE_EVENT"]


def test_nothing_is_silently_dropped():
    recs = [ev(), ev(event_id="E2", amount="-1"), ev(event_id="E3", order_id=None), ev()]
    ok, rej = check(recs)
    assert len(ok) + len(rej) == len(recs)
    assert rej["raw_record"].notna().all() and (rej["source_row"] >= 2).all()


def test_baseline_validation_catches_duplicates_and_bad_rows():
    df = pd.DataFrame([
        dict(order_id="O1", customer_id="C1", order_date="2026-01-01", amount="10", status="PAID", updated_at="2026-01-01 00:00:00"),
        dict(order_id="O1", customer_id="C1", order_date="2026-01-01", amount="10", status="PAID", updated_at="2026-01-01 00:00:00"),
        dict(order_id="O2", customer_id="C9", order_date="2026-01-01", amount="10", status="PAID", updated_at="2026-01-01 00:00:00"),
    ])
    ok, rej = validate_baseline(df, CUST, ALLOWED)
    assert len(ok) == 1 and sorted(rej["reject_reason"]) == ["DUPLICATE_ORDER_ID", "INVALID_CUSTOMER"]
