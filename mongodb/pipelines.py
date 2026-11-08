# mongodb/pipelines.py
from pymongo import MongoClient
import pandas as pd

db = MongoClient("mongodb://localhost:27017")["ecommerce"]
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", None)

# ------------------------------------------------------------------
# Pipeline 1 : Popularité des produits
# Top 10 des produits les plus vendus (quantité + revenu),
# enrichis avec nom, catégorie et nombre de transactions.
# ------------------------------------------------------------------
pipeline_top_products = [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "units_sold": {"$sum": "$items.quantity"},
        "revenue": {"$sum": "$items.subtotal"},
        "nb_orders": {"$sum": 1},
    }},
    {"$sort": {"units_sold": -1}},
    {"$limit": 10},
    {"$lookup": {
        "from": "products",
        "localField": "_id",
        "foreignField": "product_id",
        "as": "p",
    }},
    {"$unwind": "$p"},
    {"$project": {
        "_id": 0,
        "product_id": "$_id",
        "name": "$p.name",
        "category": "$p.category_name",
        "units_sold": 1,
        "nb_orders": 1,
        "revenue": {"$round": ["$revenue", 2]},
    }},
]

# ------------------------------------------------------------------
# Pipeline 2 : Revenu par catégorie, avec marge estimée
# Utilise category_id dupliqué dans les items (évite un $lookup
# par ligne) puis un $lookup unique sur les 25 catégories.
# ------------------------------------------------------------------
pipeline_revenue_category = [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.category_id",
        "revenue": {"$sum": "$items.subtotal"},
        "units": {"$sum": "$items.quantity"},
    }},
    {"$lookup": {
        "from": "categories",
        "localField": "_id",
        "foreignField": "category_id",
        "as": "c",
    }},
    {"$unwind": "$c"},
    {"$project": {
        "_id": 0,
        "category_id": "$_id",
        "category": "$c.name",
        "units": 1,
        "revenue": {"$round": ["$revenue", 2]},
        "est_profit": {"$round": [
            {"$multiply": ["$revenue", {"$avg": "$c.subcategories.profit_margin"}]}, 2]},
    }},
    {"$sort": {"revenue": -1}},
]

# ------------------------------------------------------------------
# Bonus (segmentation) : utilisateurs par tranche de dépense,
# grâce au purchase_summary embarqué dans users.
# ------------------------------------------------------------------
pipeline_segments = [
    {"$match": {"purchase_summary": {"$exists": True}}},
    {"$bucket": {
        "groupBy": "$purchase_summary.total_spent",
        "boundaries": [0, 2000, 4000, 6000, 8000, 10000, 100000],
        "default": "other",
        "output": {
            "users": {"$sum": 1},
            "avg_orders": {"$avg": "$purchase_summary.total_orders"},
            "avg_spent": {"$avg": "$purchase_summary.total_spent"},
        },
    }},
]

if __name__ == "__main__":
    print("\n=== Top 10 produits vendus ===")
    print(pd.DataFrame(db.transactions.aggregate(pipeline_top_products)))

    print("\n=== Revenu par catégorie ===")
    print(pd.DataFrame(db.transactions.aggregate(pipeline_revenue_category)).head(10))

    print("\n=== Segmentation par dépense ===")
    print(pd.DataFrame(db.users.aggregate(pipeline_segments)))