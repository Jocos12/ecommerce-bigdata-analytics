# viz/charts.py : visualisations du rapport
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

os.makedirs("report/figures", exist_ok=True)
sns.set_theme(style="whitegrid")
db = MongoClient("mongodb://localhost:27017")["ecommerce"]
OK = ["completed", "shipped", "delivered"]

# 1) Revenu par catégorie (top 10)
rev = pd.DataFrame(db.transactions.aggregate([
    {"$match": {"status": {"$in": OK}}}, {"$unwind": "$items"},
    {"$group": {"_id": "$items.category_id", "revenue": {"$sum": "$items.subtotal"}}},
    {"$lookup": {"from": "categories", "localField": "_id", "foreignField": "category_id", "as": "c"}},
    {"$unwind": "$c"},
    {"$project": {"_id": 0, "category": "$c.name", "revenue": 1}},
    {"$sort": {"revenue": -1}}, {"$limit": 10}]))
plt.figure(figsize=(9, 5))
sns.barplot(data=rev, y="category", x="revenue", color="#2a6f97")
plt.title("Revenu par catégorie (top 10)"); plt.xlabel("Revenu"); plt.ylabel("")
plt.tight_layout(); plt.savefig("report/figures/fig1_revenue_category.png", dpi=150); plt.close()

# 2) Revenu hebdomadaire (evite les mois partiels)
wk = pd.DataFrame(db.transactions.aggregate([
    {"$match": {"status": {"$in": OK}}},
    {"$group": {"_id": {"$dateTrunc": {"date": "$timestamp", "unit": "week"}},
                "revenue": {"$sum": "$total"}}},
    {"$sort": {"_id": 1}}]))
wk = wk.iloc[1:-1]  # retire les semaines partielles aux extremites
plt.figure(figsize=(9, 4.5))
plt.plot(wk["_id"], wk["revenue"], marker="o", color="#2a6f97")
plt.title("Revenu hebdomadaire (semaines complètes)"); plt.xlabel("Semaine"); plt.ylabel("Revenu")
plt.xticks(rotation=30); plt.tight_layout()
plt.savefig("report/figures/fig2_weekly_revenue.png", dpi=150); plt.close()

# 3) Segmentation par depense
seg = pd.DataFrame(db.users.aggregate([
    {"$match": {"purchase_summary": {"$exists": True}}},
    {"$bucket": {"groupBy": "$purchase_summary.total_spent",
                 "boundaries": [0, 2000, 4000, 6000, 8000, 10000, 100000], "default": "other",
                 "output": {"users": {"$sum": 1}, "avg_spent": {"$avg": "$purchase_summary.total_spent"}}}}]))
labels = ["<2k", "2-4k", "4-6k", "6-8k", "8-10k", "10k+"]
seg["segment"] = labels[:len(seg)]
fig, ax = plt.subplots(figsize=(8, 4.5))
sns.barplot(data=seg, x="segment", y="users", color="#52b69a", ax=ax)
ax.set_title("Clients par tranche de dépense totale"); ax.set_xlabel("Dépense totale"); ax.set_ylabel("Clients")
for i, r in seg.iterrows():
    ax.text(i, r["users"], f"{int(r['avg_spent']):,}".replace(",", " "), ha="center", va="bottom", fontsize=8)
plt.tight_layout(); plt.savefig("report/figures/fig3_segmentation.png", dpi=150); plt.close()

# 4) Funnel de conversion
f = pd.read_csv("results/funnel.csv")
plt.figure(figsize=(8, 4.5))
ax = sns.barplot(data=f, x="stage", y="count", color="#1d3557")
for i, r in f.iterrows():
    ax.text(i, r["count"], f"{r['count']:,}\n({r['count']/f['count'].iloc[0]*100:.1f}%)".replace(",", " "),
            ha="center", va="bottom", fontsize=9)
plt.title("Funnel de conversion (200 000 sessions)"); plt.xlabel(""); plt.ylabel("Sessions")
plt.ylim(0, f["count"].max() * 1.15); plt.tight_layout()
plt.savefig("report/figures/fig4_funnel.png", dpi=150); plt.close()

# 5) Top produits : vendus vs revenu
top = pd.DataFrame(db.transactions.aggregate([
    {"$match": {"status": {"$in": OK}}}, {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"units": -1}}, {"$limit": 10}]))
fig, ax = plt.subplots(figsize=(9, 4.5))
x = range(len(top))
ax.bar(x, top["units"], color="#2a6f97", label="Unités vendues")
ax.set_xticks(list(x)); ax.set_xticklabels(top["_id"], rotation=45, ha="right")
ax.set_ylabel("Unités"); ax2 = ax.twinx()
ax2.plot(list(x), top["revenue"], color="#e76f51", marker="o", label="Revenu")
ax2.set_ylabel("Revenu"); ax.set_title("Top 10 produits : unités vendues vs revenu")
fig.tight_layout(); fig.savefig("report/figures/fig5_top_products.png", dpi=150); plt.close()

print("Figures enregistrées dans report/figures/")