"""Streamlit dashboard (read-only view over stored results; no business logic here).
Run:  streamlit run dashboard/app.py
"""
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.config import load_config, resolve  # noqa: E402

cfg = load_config()
DB, REPORTS = resolve(cfg, "warehouse_db"), resolve(cfg, "reports_dir")

st.set_page_config(page_title="DataSync Benchmark", layout="wide")
st.title("DataSync Benchmark - Full Refresh vs Incremental Merge vs Partition Replacement")

if not DB.exists():
    st.warning("No warehouse yet. Run:  python run_pipeline.py --strategy all --size 10k")
    st.stop()

conn = sqlite3.connect(DB)
runs = pd.read_sql_query("SELECT * FROM pipeline_runs ORDER BY started_at DESC, rowid DESC", conn)
tab = st.tabs(["Overview", "Performance", "Data quality", "Reconciliation", "Run history", "Errors"])

with tab[0]:
    c = st.columns(4)
    c[0].metric("Total runs", len(runs))
    c[1].metric("Successful", int((runs.status == "SUCCESS").sum()))
    c[2].metric("Skipped (duplicate batch)", int((runs.status == "SKIPPED_DUPLICATE_BATCH").sum()))
    c[3].metric("Failed", int((runs.status == "FAILED").sum()))
    for t in ("target_full_refresh", "target_incremental", "target_partition"):
        st.write(f"`{t}`: {conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:,} rows")

with tab[1]:
    bench = REPORTS / "benchmark_report.csv"
    if bench.exists():
        b = pd.read_csv(bench)
        st.subheader("Latest benchmark (median of repeated runs on identical initial state)")
        st.dataframe(b, use_container_width=True)
        st.bar_chart(b.set_index("strategy")["median_sec"])
    ok = runs[runs.status == "SUCCESS"]
    if len(ok):
        st.subheader("Single pipeline runs")
        st.bar_chart(ok.head(9).set_index("strategy")["duration_sec"])

with tab[2]:
    f = REPORTS / "data_quality_report.json"
    if f.exists():
        dq = json.loads(f.read_text())
        c = st.columns(3)
        c[0].metric("Input", dq["input_records"]); c[1].metric("Valid", dq["valid_records"])
        c[2].metric("Rejected", dq["rejected_records"])
        st.bar_chart(pd.Series(dq["rejected_by_reason"], name="rejected"))
    st.subheader("Quarantined records")
    st.dataframe(pd.read_sql_query("SELECT reject_reason, record_type, source_row, record_key, raw_record "
                                   "FROM rejected_records LIMIT 500", conn), use_container_width=True)

with tab[3]:
    f = REPORTS / "reconciliation_report.json"
    if f.exists():
        rec = json.loads(f.read_text())
        (st.success if rec["all_strategies_equivalent"] else st.error)(rec["overall"])
        for name, r in rec["strategies"].items():
            with st.expander(f"{name}: {r['status']}"):
                st.json(r["checks"])

with tab[4]:
    st.dataframe(runs, use_container_width=True)

with tab[5]:
    st.dataframe(runs[runs.status == "FAILED"][["run_id", "strategy", "started_at", "error_message"]],
                 use_container_width=True)
