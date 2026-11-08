# spark/clean.py : nettoyage et normalisation avec PySpark
import os
os.environ["PYSPARK_PYTHON"] = "python"
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("ecom-clean")
         .master("local[2]")
         .config("spark.driver.memory", "5g")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.files.maxPartitionBytes", "32m")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

D = "data/"
users = spark.read.option("multiLine", True).json(D + "users.json")
products = spark.read.option("multiLine", True).json(D + "products.json")
trans = spark.read.json(D + "jsonl/transactions.jsonl")
sessions = spark.read.json(D + "jsonl/sessions_*.jsonl")

print("AVANT nettoyage")
for n, df in [("users", users), ("products", products),
              ("transactions", trans), ("sessions", sessions)]:
    print(f"  {n}: {df.count()} lignes")

# --- Nettoyage ---
users_c = (users.dropDuplicates(["user_id"])
           .withColumn("registration_date", F.to_timestamp("registration_date"))
           .withColumn("last_active", F.to_timestamp("last_active"))
           .withColumn("country", F.upper(F.col("geo_data.country")))
           .withColumn("state", F.col("geo_data.state"))
           .withColumn("city", F.col("geo_data.city")))

products_c = (products.dropDuplicates(["product_id"])
              .withColumn("creation_date", F.to_timestamp("creation_date"))
              .filter(F.col("base_price") > 0))

trans_c = (trans.dropDuplicates(["transaction_id"])
           .withColumn("ts", F.to_timestamp("timestamp"))
           .withColumn("has_session", F.col("session_id").isNotNull())
           .filter(F.col("total") >= 0))

sessions_c = (sessions.dropDuplicates(["session_id"])
              .withColumn("start_ts", F.to_timestamp("start_time"))
              .withColumn("n_pages", F.size("page_views"))
              .filter(F.col("duration_seconds") > 0))

print("APRES nettoyage")
for n, df in [("users", users_c), ("products", products_c),
              ("transactions", trans_c), ("sessions", sessions_c)]:
    print(f"  {n}: {df.count()} lignes")

print("Transactions sans session :", trans_c.filter(~F.col("has_session")).count())
print("Statuts :")
trans_c.groupBy("status").count().show()

# Sauvegarde Parquet pour les jobs suivants
os.makedirs("data/clean", exist_ok=True)
users_c.write.mode("overwrite").parquet("data/clean/users")
products_c.write.mode("overwrite").parquet("data/clean/products")
trans_c.write.mode("overwrite").parquet("data/clean/transactions")
# Sessions : colonnes légères
sessions_light = sessions_c.select(
    "session_id", "user_id", "start_ts", "duration_seconds", "n_pages",
    "conversion_status", "referrer",
    F.col("device_profile.type").alias("device"),
    F.col("viewed_products"),
)
sessions_light.coalesce(4).write.mode("overwrite").parquet("data/clean/sessions")

# Table des vues produit (une ligne par page product_detail)
pv = (sessions_c.select("session_id", "user_id", F.explode("page_views").alias("pv"))
      .filter(F.col("pv.page_type") == "product_detail")
      .select("session_id", "user_id",
              F.col("pv.product_id").alias("product_id"),
              F.to_timestamp("pv.timestamp").alias("ts")))
pv.coalesce(4).write.mode("overwrite").parquet("data/clean/product_views")
print("Parquet écrit dans data/clean/")
spark.stop()