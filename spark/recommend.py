# spark/recommend.py : "les utilisateurs qui ont acheté X ont aussi acheté Y"
import os
os.environ["PYSPARK_PYTHON"] = "python"
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("ecom-recommend")
         .master("local[2]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

trans = spark.read.parquet("data/clean/transactions")
products = spark.read.parquet("data/clean/products").select("product_id", "name")

# Une ligne par (transaction, produit) : on garde les achats finalisés
lines = (trans.filter(F.col("status").isin("completed", "shipped", "delivered"))
         .select("transaction_id", F.explode("items").alias("it"))
         .select("transaction_id", F.col("it.product_id").alias("product_id"))
         .distinct())

# Auto-jointure : paires de produits dans la même transaction (a < b évite les doublons)
a, b = lines.alias("a"), lines.alias("b")
pairs = (a.join(b, "transaction_id")
         .filter(F.col("a.product_id") < F.col("b.product_id"))
         .select(F.col("a.product_id").alias("p1"), F.col("b.product_id").alias("p2")))

pair_counts = pairs.groupBy("p1", "p2").agg(F.count("*").alias("co_purchases"))

# Support de chaque produit (nb de transactions contenant le produit)
n_tx = lines.select("transaction_id").distinct().count()
support = lines.groupBy("product_id").agg(F.count("*").alias("n"))

# Lift = P(A et B) / (P(A) * P(B))
sa = support.withColumnRenamed("product_id", "p1").withColumnRenamed("n", "n1")
sb = support.withColumnRenamed("product_id", "p2").withColumnRenamed("n", "n2")
rec = (pair_counts.join(sa, "p1").join(sb, "p2")
       .withColumn("lift", (F.col("co_purchases") * n_tx) / (F.col("n1") * F.col("n2")))
       .filter(F.col("co_purchases") >= 2))

named = (rec.join(products.withColumnRenamed("product_id", "p1").withColumnRenamed("name", "name1"), "p1")
         .join(products.withColumnRenamed("product_id", "p2").withColumnRenamed("name", "name2"), "p2")
         .select("p1", "name1", "p2", "name2", "co_purchases", F.round("lift", 1).alias("lift"))
         .orderBy(F.desc("co_purchases"), F.desc("lift")))

print(f"Transactions finalisées analysées : {n_tx}")
print(f"Paires de produits distinctes : {pair_counts.count()}")
named.show(15, truncate=40)

os.makedirs("results", exist_ok=True)
named.limit(200).toPandas().to_csv("results/product_pairs.csv", index=False)
print("Exporté : results/product_pairs.csv")
spark.stop()