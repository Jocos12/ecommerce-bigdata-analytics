# spark/sql_analytics.py : requêtes Spark SQL
import os
os.environ["PYSPARK_PYTHON"] = "python"
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("ecom-sql")
         .master("local[2]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

for name in ["users", "products", "transactions", "sessions", "product_views"]:
    spark.read.parquet(f"data/clean/{name}").createOrReplaceTempView(name)

# Les catégories restent en JSON (petit fichier, tableau imbriqué)
spark.read.option("multiLine", True).json("data/categories.json").createOrReplaceTempView("categories_raw")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW subcategories AS
    SELECT category_id, name AS category_name, s.subcategory_id,
           s.name AS subcategory_name, s.profit_margin
    FROM categories_raw LATERAL VIEW explode(subcategories) t AS s
""")

# Q1 : Revenu mensuel et panier moyen (fenêtre : évolution mois sur mois)
print("\n=== Q1 : Revenu mensuel et croissance ===")
spark.sql("""
    WITH m AS (
        SELECT date_format(ts, 'yyyy-MM') AS month,
               COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue,
               ROUND(AVG(total), 2) AS avg_basket
        FROM transactions
        WHERE status IN ('completed','shipped','delivered')
        GROUP BY 1)
    SELECT month, orders, revenue, avg_basket,
           ROUND((revenue - LAG(revenue) OVER (ORDER BY month))
                 / LAG(revenue) OVER (ORDER BY month) * 100, 1) AS growth_pct
    FROM m ORDER BY month
""").show()

# Q2 : Marge réelle par sous-catégorie (jointure transactions x produits x sous-catégories)
print("\n=== Q2 : Top 10 sous-catégories par profit estimé ===")
spark.sql("""
    WITH items AS (
        SELECT it.product_id AS product_id, it.subtotal AS subtotal
        FROM transactions t
        LATERAL VIEW explode(t.items) x AS it
        WHERE t.status IN ('completed','shipped','delivered')
    )
    SELECT sc.category_name, sc.subcategory_name,
           ROUND(SUM(i.subtotal), 2) AS revenue,
           ROUND(SUM(i.subtotal * sc.profit_margin), 2) AS est_profit
    FROM items i
    JOIN products p ON p.product_id = i.product_id
    JOIN subcategories sc ON sc.subcategory_id = p.subcategory_id
    GROUP BY 1, 2
    ORDER BY est_profit DESC
    LIMIT 10
""").show(truncate=35)

# Q3 : Taux de conversion par appareil et source de trafic
print("\n=== Q3 : Conversion par appareil et referrer ===")
spark.sql("""
    SELECT device, referrer, COUNT(*) AS sessions,
           ROUND(AVG(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END)*100, 2) AS conv_rate_pct,
           ROUND(AVG(duration_seconds), 0) AS avg_duration
    FROM sessions
    GROUP BY device, referrer
    ORDER BY conv_rate_pct DESC
    LIMIT 10
""").show()

# Q4 : Produits très vus et leur taux de vue-vers-achat
print("\n=== Q4 : Produits les plus vus et leur taux de vue-vers-achat ===")
spark.sql("""
    WITH v AS (
        SELECT product_id, COUNT(*) AS views FROM product_views GROUP BY 1
    ),
    items AS (
        SELECT it.product_id AS product_id, it.quantity AS quantity
        FROM transactions t
        LATERAL VIEW explode(t.items) x AS it
        WHERE t.status IN ('completed','shipped','delivered')
    ),
    b AS (
        SELECT product_id, SUM(quantity) AS units FROM items GROUP BY 1
    )
    SELECT v.product_id, v.views, COALESCE(b.units, 0) AS units,
           ROUND(COALESCE(b.units, 0) / v.views * 100, 1) AS purchase_per_100_views
    FROM v LEFT JOIN b ON v.product_id = b.product_id
    ORDER BY v.views DESC LIMIT 10
""").show()

spark.stop()