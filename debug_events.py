import sqlite3

conn = sqlite3.connect("data/warehouse/benchmark.db")

query = """
SELECT
    event_id,
    COUNT(DISTINCT batch_id) AS batch_count,
    GROUP_CONCAT(DISTINCT batch_id) AS batches
FROM order_events
GROUP BY event_id
HAVING COUNT(DISTINCT batch_id) > 1
"""

results = conn.execute(query).fetchall()

print("\n========== CROSS-BATCH COLLISIONS ==========")
print("Total collisions:", len(results))

for row in results[:20]:
    print(row)

conn.close()
'''
import sqlite3

DB_PATH = "data/warehouse/benchmark.db"

conn = sqlite3.connect(DB_PATH)

query = """
SELECT
    batch_id,
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_events
FROM order_events
GROUP BY batch_id
"""

results = conn.execute(query).fetchall()

print("\n========== EVENT HISTORY ==========")

for batch_id, total, unique in results:
    print(f"Batch ID       : {batch_id}")
    print(f"Total Events   : {total}")
    print(f"Unique Events  : {unique}")
    print(f"Duplicates     : {total - unique}")
    print("----------------------------------")

conn.close()
'''