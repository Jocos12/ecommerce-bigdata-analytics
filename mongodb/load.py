# mongodb/load.py
import json
import os
from datetime import datetime
from pymongo import MongoClient

DATA_DIR = "data"
DB_NAME = "ecommerce"
SESSIONS_LIMIT = 50000   # sous-ensemble de sessions pour MongoDB
BATCH = 5000

client = MongoClient("mongodb://localhost:27017")
db = client[DB_NAME]


def load(name):
    with open(os.path.join(DATA_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def dt(s):
    return datetime.fromisoformat(s) if s else None


def insert_batches(coll, docs):
    for i in range(0, len(docs), BATCH):
        coll.insert_many(docs[i:i + BATCH], ordered=False)


# On repart de zéro à chaque exécution
client.drop_database(DB_NAME)

# --- Categories ---
categories = load("categories.json")
db.categories.insert_many(categories)
sub_lookup = {}
for c in categories:
    for s in c["subcategories"]:
        sub_lookup[s["subcategory_id"]] = (c["name"], s["name"], s["profit_margin"])
print(f"categories: {db.categories.count_documents({})}")

# --- Products (dénormalisation catégorie + sous-catégorie) ---
products = load("products.json")
prod_cat = {}
for p in products:
    cname, sname, margin = sub_lookup[p["subcategory_id"]]
    p["category_name"] = cname
    p["subcategory_name"] = sname
    p["profit_margin"] = margin
    p["creation_date"] = dt(p["creation_date"])
    for ph in p["price_history"]:
        ph["date"] = dt(ph["date"])
    prod_cat[p["product_id"]] = p["category_id"]
insert_batches(db.products, products)
print(f"products: {db.products.count_documents({})}")

# --- Transactions (items embarqués + category_id dupliqué) ---
transactions = load("transactions.json")
for t in transactions:
    t["timestamp"] = dt(t["timestamp"])
    for it in t["items"]:
        it["category_id"] = prod_cat.get(it["product_id"])
insert_batches(db.transactions, transactions)
print(f"transactions: {db.transactions.count_documents({})}")

# --- Users ---
users = load("users.json")
for u in users:
    u["registration_date"] = dt(u["registration_date"])
    u["last_active"] = dt(u["last_active"])
insert_batches(db.users, users)
print(f"users: {db.users.count_documents({})}")

# --- Sessions (sous-ensemble) ---
sessions = load("sessions_0.json")[:SESSIONS_LIMIT]
for s in sessions:
    s["start_time"] = dt(s["start_time"])
    s["end_time"] = dt(s["end_time"])
    for pv in s["page_views"]:
        pv["timestamp"] = dt(pv["timestamp"])
insert_batches(db.sessions, sessions)
print(f"sessions: {db.sessions.count_documents({})}")

# --- Index ---
db.products.create_index("product_id", unique=True)
db.products.create_index("category_id")
db.users.create_index("user_id", unique=True)
db.transactions.create_index("user_id")
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")
db.sessions.create_index("user_id")
db.sessions.create_index("session_id", unique=True)

# --- Résumé d'achats embarqué dans users ---
print("Calcul du résumé d'achats...")
for doc in db.transactions.aggregate([
    {"$group": {
        "_id": "$user_id",
        "total_orders": {"$sum": 1},
        "total_spent": {"$sum": "$total"},
        "first_purchase": {"$min": "$timestamp"},
        "last_purchase": {"$max": "$timestamp"},
    }}
]):
    db.users.update_one(
        {"user_id": doc["_id"]},
        {"$set": {"purchase_summary": {
            "total_orders": doc["total_orders"],
            "total_spent": round(doc["total_spent"], 2),
            "avg_order_value": round(doc["total_spent"] / doc["total_orders"], 2),
            "first_purchase": doc["first_purchase"],
            "last_purchase": doc["last_purchase"],
        }}},
    )
print("Terminé.")