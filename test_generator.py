from pathlib import Path
import pandas as pd

from src.ingestion.data_generator import generate

# Generate batch 1
generate(
    size=1000,
    base_dir="debug_data/batch1",
    seed=42,
    batch_id="batch_001"
)

# Generate batch 2
generate(
    size=1000,
    base_dir="debug_data/batch2",
    seed=42,
    batch_id="batch_002"
)

df1 = pd.read_csv(
    "debug_data/batch1/incoming/order_events.csv"
)

df2 = pd.read_csv(
    "debug_data/batch2/incoming/order_events.csv"
)

ids1 = set(df1["event_id"])
ids2 = set(df2["event_id"])

print("Batch 1 events:", len(df1))
print("Batch 2 events:", len(df2))

print("Batch 1 unique IDs:", len(ids1))
print("Batch 2 unique IDs:", len(ids2))

print("Cross-batch collisions:", len(ids1 & ids2))

print(
    "Batch 1 duplicate deliveries:",
    len(df1) - df1["event_id"].nunique()
)

print(
    "Batch 2 duplicate deliveries:",
    len(df2) - df2["event_id"].nunique()
)