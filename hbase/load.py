# hbase/load.py
import json
import sys
from datetime import datetime
import happybase

SESSIONS_FILE = "data/sessions_0.json"
LIMIT = 50000          # sous-ensemble de sessions
MAX_TS = 9999999999    # pour le reverse timestamp (secondes)

conn = happybase.Connection("localhost", port=9090, timeout=60000)
conn.open()
sessions_tbl = conn.table("user_sessions")
metrics_tbl = conn.table("product_metrics")


def b(x):
    return str(x).encode("utf-8")


print("Lecture des sessions...")
with open(SESSIONS_FILE, encoding="utf-8") as f:
    sessions = json.load(f)[:LIMIT]

# Agrégats produit/jour : (product_id, yyyymmdd) -> compteurs
metrics = {}


def bump(pid, day, field, n=1):
    key = (pid, day)
    metrics.setdefault(key, {"views": 0, "cart_adds": 0, "purchases": 0})
    metrics[key][field] += n


print("Chargement dans user_sessions...")
batch = sessions_tbl.batch(batch_size=2000)
for i, s in enumerate(sessions, 1):
    start = datetime.fromisoformat(s["start_time"])
    ts = int(start.timestamp())
    rev = MAX_TS - ts
    row_key = f"{s['user_id']}_{rev:010d}".encode()
    day = start.strftime("%Y%m%d")

    data = {
        b"info:session_id": b(s["session_id"]),
        b"info:start_time": b(s["start_time"]),
        b"info:duration": b(s["duration_seconds"]),
        b"info:device": b(s["device_profile"]["type"]),
        b"info:os": b(s["device_profile"]["os"]),
        b"info:referrer": b(s["referrer"]),
        b"info:conversion": b(s["conversion_status"]),
        b"info:n_pages": b(len(s["page_views"])),
    }
    # Une colonne par page vue : pages:<index>
    for idx, pv in enumerate(s["page_views"]):
        val = f"{pv['timestamp']}|{pv['page_type']}|{pv['product_id']}|{pv['view_duration']}"
        data[f"pages:{idx:02d}".encode()] = val.encode()
        if pv["page_type"] == "product_detail" and pv["product_id"]:
            bump(pv["product_id"], day, "views")
    for pid in s["cart_contents"]:
        bump(pid, day, "cart_adds")
        if s["conversion_status"] == "converted":
            bump(pid, day, "purchases")

    batch.put(row_key, data)
    if i % 10000 == 0:
        print(f"  {i} sessions")
batch.send()

print("Chargement dans product_metrics...")
batch = metrics_tbl.batch(batch_size=5000)
for (pid, day), m in metrics.items():
    batch.put(f"{pid}_{day}".encode(), {
        b"m:views": b(m["views"]),
        b"m:cart_adds": b(m["cart_adds"]),
        b"m:purchases": b(m["purchases"]),
    })
batch.send()

print(f"Terminé : {len(sessions)} sessions, {len(metrics)} lignes de métriques.")
conn.close()