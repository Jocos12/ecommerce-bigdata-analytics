# integration/funnel.py : funnel vues -> panier -> achat
# Sources : sessions (modele HBase), transactions (MongoDB), jointure dans Spark
import os
os.environ["PYSPARK_PYTHON"] = "python"
from pyspark.sql import SparkSession, functions as F
from pymongo import MongoClient

spark = (SparkSession.builder.appName("ecom-funnel").master("local[2]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1) Sessions (equivalent des donnees navigation stockees dans HBase)
sessions = spark.read.parquet("data/clean/sessions")

# 2) Transactions finalisees lues depuis MongoDB (via pymongo -> DataFrame Spark)
db = MongoClient("mongodb://localhost:27017")["ecommerce"]
rows = list(db.transactions.find(
    {"status": {"$in": ["completed", "shipped", "delivered"]}, "session_id": {"$ne": None}},
    {"_id": 0, "session_id": 1, "total": 1}))
tx = spark.createDataFrame(rows).withColumnRenamed("total", "order_total")

# 3) Jointure sessions x achats
s = sessions.join(tx, "session_id", "left").withColumn(
    "has_view", F.size("viewed_products") > 0).withColumn(
    "has_cart", F.col("conversion_status").isin("abandoned", "converted")).withColumn(
    "has_purchase", F.col("order_total").isNotNull())

# 4) Funnel global
total = s.count()
v = s.filter("has_view").count()
c = s.filter("has_view AND has_cart").count()
p = s.filter("has_purchase").count()
funnel = [("Sessions", total), ("Vue produit", v), ("Ajout panier", c), ("Achat", p)]
print("\n=== Funnel global ===")
for name, n in funnel:
    print(f"{name:<14}{n:>8}  ({n/total*100:5.1f}% des sessions)")

# 5) Funnel par appareil et par referrer
for dim in ["device", "referrer"]:
    print(f"\n=== Funnel par {dim} ===")
    (s.groupBy(dim).agg(
        F.count("*").alias("sessions"),
        F.sum(F.col("has_view").cast("int")).alias("views"),
        F.sum((F.col("has_view") & F.col("has_cart")).cast("int")).alias("carts"),
        F.sum(F.col("has_purchase").cast("int")).alias("purchases"),
        F.round(F.sum("order_total"), 0).alias("revenue"))
     .withColumn("cart_to_purchase_pct", F.round(F.col("purchases") / F.col("carts") * 100, 1))
     .orderBy(F.desc("revenue")).show())

# Export pour les graphiques
os.makedirs("results", exist_ok=True)
import pandas as pd
pd.DataFrame(funnel, columns=["stage", "count"]).to_csv("results/funnel.csv", index=False)
(s.groupBy("device").agg(F.count("*").alias("sessions"),
    F.sum(F.col("has_purchase").cast("int")).alias("purchases"))
 .toPandas().to_csv("results/funnel_by_device.csv", index=False))
print("Exporte : results/funnel.csv, results/funnel_by_device.csv")
spark.stop()