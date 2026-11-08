# hbase/queries.py
import sys
import happybase

conn = happybase.Connection("localhost", port=9090, timeout=60000)
sessions = conn.table("user_sessions")
metrics = conn.table("product_metrics")


def decode(row):
    return {k.decode(): v.decode() for k, v in row.items()}


def user_sessions(user_id, limit=5):
    """Sessions les plus récentes d'un utilisateur (scan par préfixe)."""
    out = []
    for key, data in sessions.scan(
        row_prefix=f"{user_id}_".encode(),
        columns=[b"info"],
        limit=limit,
    ):
        d = decode(data)
        out.append((key.decode(), d))
    return out


def user_session_pages(user_id):
    """Détail des pages vues de la session la plus récente."""
    for key, data in sessions.scan(row_prefix=f"{user_id}_".encode(), limit=1):
        return key.decode(), decode(data)
    return None, {}


def product_timeline(product_id):
    """Évolution quotidienne d'un produit (scan de plage sur product_id_date)."""
    rows = []
    for key, data in metrics.scan(
        row_start=f"{product_id}_".encode(),
        row_stop=f"{product_id}_99999999".encode(),
    ):
        d = decode(data)
        rows.append((key.decode().split("_")[-1], d["m:views"], d["m:cart_adds"], d["m:purchases"]))
    return rows


if __name__ == "__main__":
    # Prend un user qui a des sessions dans le sous-ensemble chargé
    first_key = next(sessions.scan(limit=1))[0].decode()
    uid = first_key.rsplit("_", 1)[0]
    print(f"\n=== 5 dernières sessions de {uid} ===")
    for key, d in user_sessions(uid):
        print(key, d["info:start_time"], d["info:device"], d["info:referrer"], d["info:conversion"])

    key, pages = user_session_pages(uid)
    print(f"\n=== Pages de la session la plus récente ({key}) ===")
    for col in sorted(c for c in pages if c.startswith("pages:")):
        print(col, pages[col])

    pid = "prod_01076"
    print(f"\n=== Évolution quotidienne de {pid} (extrait) ===")
    print("date      views cart purchases")
    for row in product_timeline(pid)[:10]:
        print(*row)