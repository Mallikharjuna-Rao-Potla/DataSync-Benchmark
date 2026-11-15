import sqlite3
import pandas as pd
from pathlib import Path

conn = sqlite3.connect("data/warehouse/benchmark.db")

# Find incoming event file
files = list(Path("data").rglob("order_events.csv"))

if not files:
    print("Incoming event file not found.")
    raise SystemExit

file = files[0]
incoming = pd.read_csv(file, dtype=str)

existing_ids = {
    row[0] for row in conn.execute(
        "SELECT event_id FROM order_events"
    )
}

incoming_ids = set(incoming["event_id"].dropna())

collisions = incoming_ids.intersection(existing_ids)

print("Incoming file:", file)
print("Incoming unique IDs:", len(incoming_ids))
print("Existing IDs:", len(existing_ids))
print("Overlapping IDs:", len(collisions))
print("Examples:", list(collisions)[:10])

conn.close()