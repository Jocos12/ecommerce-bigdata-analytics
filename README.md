# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics, final project. MongoDB + HBase + Apache Spark.

## Architecture
- **MongoDB**: products, categories, users (with embedded purchase summary), transactions (embedded items), sessions subset.
- **HBase**: `user_sessions` (row key `user_id_reverseTs`), `product_metrics` (row key `product_id_date`).
- **Spark (PySpark)**: cleaning, product-pair recommendations, Spark SQL, integrated funnel analysis.

## Setup
```bash
python -m venv venv && venv\Scripts\activate
pip install faker pandas numpy pymongo happybase pyspark matplotlib seaborn plotly
docker compose up -d
docker exec -d ecom-hbase hbase thrift start -p 9090
```
Add `127.0.0.1 hbase` to the hosts file.

## Reproduce (in order)
```bash
python generator/dataset_generator.py      # 200k sessions, 50k transactions
python mongodb/load.py
python mongodb/pipelines.py
# HBase tables (hbase shell): see hbase/schema.hbase
python hbase/load.py
python hbase/queries.py
python spark/to_jsonl.py
python spark/clean.py
python spark/recommend.py
python spark/sql_analytics.py
python integration/funnel.py
python viz/charts.py
```

## Layout
`generator/ mongodb/ hbase/ spark/ integration/ viz/ results/ report/`