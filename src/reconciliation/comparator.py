"""Prove the three strategies are equivalent -- record by record, not just by count."""
import hashlib

import pandas as pd

COLS = ["order_id", "customer_id", "order_date", "amount", "status", "updated_at"]


def _norm(df):
    """Sorted, typed copy. Idempotent: an already-normalised frame is returned as-is (saves
    memory -- reconciling 500k rows otherwise made ~6 copies per strategy)."""
    if df.attrs.get("normalised"):
        return df
    d = df[COLS].copy()
    d["amount"] = d["amount"].astype("int64")
    d = d.sort_values("order_id").reset_index(drop=True)
    d.attrs["normalised"] = True
    return d


def record_hash(df):
    d = _norm(df)
    return hashlib.sha256(d.to_csv(index=False).encode()).hexdigest()


def compare(name, target, expected, sample=5):
    """Compare one target to the expected final state. Returns a dict of check results."""
    t, e = _norm(target), _norm(expected)
    t_ids, e_ids = set(t["order_id"]), set(e["order_id"])
    missing, extra = sorted(e_ids - t_ids), sorted(t_ids - e_ids)
    both = t.merge(e, on="order_id", suffixes=("_t", "_e"))
    diff_mask = pd.Series(False, index=both.index)
    for c in COLS[1:]:
        diff_mask |= both[f"{c}_t"] != both[f"{c}_e"]
    mism = both[diff_mask]
    agg = {
        "sum_amount": (int(t["amount"].sum()), int(e["amount"].sum())),
        "by_status": (t.groupby("status").size().to_dict(), e.groupby("status").size().to_dict()),
        "by_date": (t.groupby("order_date").size().to_dict(), e.groupby("order_date").size().to_dict()),
    }
    checks = {
        "row_count": {"pass": len(t) == len(e), "target": len(t), "expected": len(e)},
        "primary_key_unique": {"pass": not t["order_id"].duplicated().any(),
                               "duplicates": int(t["order_id"].duplicated().sum())},
        "missing_records": {"pass": not missing, "count": len(missing), "sample": missing[:sample]},
        "extra_records": {"pass": not extra, "count": len(extra), "sample": extra[:sample]},
        "value_mismatches": {"pass": mism.empty, "count": len(mism),
                             "sample": mism["order_id"].head(sample).tolist()},
        "aggregates": {"pass": all(a == b for a, b in agg.values())},
        "record_hash": {"pass": record_hash(t) == record_hash(e)},
    }
    return {"strategy": name, "status": "PASS" if all(c["pass"] for c in checks.values()) else "FAIL",
            "checks": checks}


def reconcile(targets, expected):
    """targets: {strategy_name: DataFrame}. Returns report with per-strategy + cross checks."""
    expected = _norm(expected)
    per, hashes = {}, {}
    for n in list(targets):
        t = _norm(targets.pop(n))              # release the raw frame as we go
        per[n] = compare(n, t, expected)
        hashes[n] = record_hash(t)
        del t
    equivalent = len(set(hashes.values())) <= 1 and all(r["status"] == "PASS" for r in per.values())
    return {"expected_rows": len(expected), "strategies": per, "record_hashes": hashes,
            "all_strategies_equivalent": equivalent,
            "overall": "ALL STRATEGIES EQUIVALENT" if equivalent else "MISMATCH DETECTED"}
