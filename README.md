# Distributed Multi-Model Analytics for E-commerce Data

**AUCA Big Data Analytics, Final Project (individual)**
MongoDB (document model) + HBase (wide-column model) + Apache Spark (distributed processing)

---

## 1. Project overview

This project builds a multi-faceted analytics system on synthetic e-commerce data (users, products, categories, browsing sessions, transactions) covering 90 days of activity. Each technology is used where its data model fits best:

| Technology | Role in this project | Why it fits |
|---|---|---|
| **MongoDB** | Product catalog, user profiles, transactions with embedded line items, a subset of sessions | Nested, self-contained documents; powerful aggregation framework for business analytics |
| **HBase** | Time-series browsing data and daily product metrics | Sorted row keys give fast range scans per user or per product; sparse, wide rows suit page-view events |
| **Apache Spark** | Cleaning, product-pair recommendations, Spark SQL analytics, cross-system funnel analysis | Distributed processing of large JSON files, joins across sources, SQL over DataFrames |

## 2. Dataset

Generated with the instructor's generator, adapted in `generator/dataset_generator.py`.

| Entity | Volume | Notes |
|---|---|---|
| Users | 10,000 | Geo data, registration and last-active dates |
| Categories | 25 | 3 to 5 subcategories each, with a profit margin |
| Products | 5,000 | Price history, stock, active flag |
| Sessions | 200,000 | Stored in `sessions_0.json` and `sessions_1.json` (100k each), nested `page_views` |
| Transactions | 50,000 | Embedded `items`, discount, payment method, status |

Changes made to the original generator:
1. **Reduced volume** (2M sessions / 500k transactions in the original) so that it runs on a standard laptop. The logic is unchanged.
2. **Added `subcategory_id` to products** (it is shown in the assignment's sample data but missing from the original script).
3. Output files are written to `data/`.

Data-quality observations: about **24,796 transactions (about 50%) have no `session_id`**. They come from the generator's "additional transactions" branch and are excluded from funnel analysis.

> `data/` is **not versioned** (about 400 MB, above GitHub's file limit). Regenerate it with `python generator/dataset_generator.py`. Dates are relative to the generation day, so results will differ slightly between runs.

## 3. Architecture

```
                 data/*.json (generator)
                        |
      +-----------------+------------------+
      |                 |                  |
  MongoDB            HBase              Spark (PySpark)
  products           user_sessions      clean -> Parquet
  categories         product_metrics    recommendations
  users                                 Spark SQL
  transactions   <--- transactions ---> funnel (integration)
  sessions (50k)       sessions
```

- **MongoDB and HBase** run in Docker (`docker-compose.yml`).
- **Spark** runs locally (`local[2]`) and reads cleaned Parquet files produced by `spark/clean.py`.
- **Integration**: `integration/funnel.py` reads transactions from MongoDB, joins them with session data in Spark, and computes the conversion funnel.

## 4. Data modeling decisions

### 4.1 MongoDB (database `ecommerce`)

| Collection | Design | Justification |
|---|---|---|
| `categories` | One document per category with an embedded `subcategories` array | Small, rarely updated reference data |
| `products` | Product document with **denormalized** `category_name`, `subcategory_name`, `profit_margin`, and embedded `price_history` | One read gives everything needed to display a product or compute its margin, without joins |
| `transactions` | Embedded `items` array; `category_id` **duplicated** inside each item | A purchase is always read with its line items. Duplicating the category avoids one `$lookup` per line in revenue-by-category pipelines |
| `users` | Profile + embedded `purchase_summary` (orders, total spent, average order value, first and last purchase) | Segmentation reads a single collection instead of re-scanning all transactions |
| `sessions` | 50,000 sessions with embedded `page_views` | A full user journey fits in one document |

Indexes: unique on `product_id`, `user_id`, `session_id`; plain on `category_id`, `transactions.user_id`, `transactions.timestamp`, `transactions.items.product_id`, `sessions.user_id`.

ISO date strings are converted to real `datetime` values so time-based aggregations work.

### 4.2 HBase

| Table | Row key | Column families | Query pattern |
|---|---|---|---|
| `user_sessions` | `user_id` + `_` + **reverse timestamp** (`9999999999 - epoch seconds`, zero-padded) | `info` (session id, start time, duration, device, OS, referrer, conversion, page count), `pages` (one column per page view: `pages:00`, `pages:01`, ...) | Latest sessions of one user via a **prefix scan** |
| `product_metrics` | `product_id` + `_` + `YYYYMMDD` | `m` (`views`, `cart_adds`, `purchases`) | Daily evolution of one product via a **range scan** |

Design rationale:
- HBase sorts rows lexicographically. With a reverse timestamp, the **most recent session of a user comes first**, so `scan` with `limit` returns the latest activity directly.
- Separating `info` from `pages` lets a query read only lightweight session metadata without loading every page event.
- Sparse storage: sessions with different numbers of page views simply have different numbers of columns, with no null padding.

### 4.3 Why some data is in MongoDB and some in HBase

| Data | Store | Reason |
|---|---|---|
| Catalog, profiles, transactions | MongoDB | Rich queries, aggregations, nested documents |
| Per-user activity streams, product-per-day metrics | HBase | Time-ordered keys, high write volume, range scans |
| Cross-source joins and heavy computation | Spark | Distributed execution, SQL, window functions |

## 5. Implementation by part

### Part 1: Storage and queries
- `mongodb/load.py` loads all collections, builds indexes and computes the embedded purchase summaries.
- `mongodb/pipelines.py` contains three aggregation pipelines:
  1. **Top 10 products** by units sold (with `$unwind`, `$group`, `$lookup`).
  2. **Revenue by category** with estimated profit from the subcategory margins.
  3. **User segmentation** by total spend using `$bucket`.
- `hbase/schema.hbase` creates both tables. `hbase/load.py` loads 50,000 sessions (142,539 product-day metric rows). `hbase/queries.py` retrieves the latest sessions of a user, the page views of the most recent session, and a product's daily timeline.

### Part 2: Spark
- `spark/to_jsonl.py` converts the large JSON arrays to **JSON Lines**. Reading 188 MB single-array files with `multiLine` caused `OutOfMemoryError`, because one task must parse the whole array. JSON Lines can be split and read in parallel.
- `spark/clean.py` removes duplicates, parses timestamps, normalizes fields, filters invalid values, flattens geo data, and writes Parquet to `data/clean/` (sessions are stored in a light version plus a separate `product_views` table).
- `spark/recommend.py` computes "bought together" pairs with a self-join on transactions, then **support and lift**.
- `spark/sql_analytics.py` runs four Spark SQL queries: monthly revenue with `LAG` growth, profit by subcategory (multi-table join), conversion by device and referrer, and view-to-purchase ratio per product.

### Part 3: Integrated analytics
`integration/funnel.py` answers: **where do we lose visitors between product view, cart and purchase?**
- Browsing data: session records (modeled in HBase, read from Parquet for Spark).
- Purchases: transactions read from **MongoDB** with `pymongo`.
- Join and aggregation: **Spark**, on `session_id`, globally and by device and referrer.

### Part 4: Visualizations
`viz/charts.py` produces five figures in `report/figures/`: revenue by category, weekly revenue, spend segmentation, conversion funnel, top products (units vs revenue).

## 6. Key results

| Analysis | Result |
|---|---|
| Top products | The 10 best sellers are very close (69 to 75 units): no clear star product |
| Revenue by category | Top category about 2.12 M (Mitchell-Kim). Best estimated profit: Rodriguez-Graham (about 736 K), showing that revenue alone does not rank categories well |
| Segmentation | Spend is spread across 6 brackets, from under 2,000 to over 10,000 per user; larger spenders place more orders (about 2 to 9 on average) |
| Funnel (200,000 sessions) | Product view 98.7%, cart 64.9%, purchase 12.6%. Only about **19.4% of carts convert**, so cart abandonment is the main loss |
| Device and source | Conversion is almost identical across devices and referrers (about 19% to 21%) |
| Recommendations | 43,726 finalized transactions, 67,896 distinct product pairs, but most pairs co-occur only twice |

## 7. Scalability considerations

- **MongoDB**: shard on `user_id` (or a hashed key); the indexes above support the main queries; embedded documents keep reads to one round trip.
- **HBase**: the reverse-timestamp key spreads reads per user; salting or pre-splitting regions would avoid hotspots on very large datasets.
- **Spark**: Parquet storage and JSON Lines allow parallel reads; moving from `local[2]` to a cluster needs no code change. Shuffle partitions are tuned for local runs.
- **Integration**: fetching transactions through the driver (about 25,000 rows) is fine here; at larger scale use the MongoDB and HBase Spark connectors.

## 8. Limitations and future work

- The data is **synthetic and randomly generated**: device, referrer and conversion attributes are independent, so segment differences are not meaningful, and product-pair lifts are inflated by rare co-occurrences. A minimum support threshold (for example 5 co-purchases) should be used on real data.
- Only **50,000 sessions** are loaded into MongoDB and HBase (hardware limit); Spark processes all 200,000.
- **Monthly growth figures are artifacts**: the first and last months are partial (90-day window).
- Spark reads sessions from Parquet instead of HBase directly (no connector set up). Future work: Spark-HBase and MongoDB Spark connectors, a cohort analysis, simulated streaming with Spark Structured Streaming, collaborative filtering (ALS).

## 9. How to run

### Prerequisites
Python 3.12, Docker Desktop, Java 17 or 21 (for Spark).

### Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install faker pandas numpy pymongo happybase pyspark matplotlib seaborn plotly
docker compose up -d
docker exec -d ecom-hbase hbase thrift start -p 9090
```
Add `127.0.0.1 hbase` to `C:\Windows\System32\drivers\etc\hosts` (administrator rights) so `happybase` can resolve the HBase host.

### Reproduce (in order)
```bash
python generator/dataset_generator.py      # 200k sessions, 50k transactions
python mongodb/load.py                     # load MongoDB
python mongodb/pipelines.py                # MongoDB aggregations
# create HBase tables: docker exec -it ecom-hbase hbase shell, then paste hbase/schema.hbase
python hbase/load.py                       # load HBase
python hbase/queries.py                    # HBase queries
python spark/to_jsonl.py                   # JSON -> JSON Lines
python spark/clean.py                      # cleaning -> Parquet
python spark/recommend.py                  # product pairs
python spark/sql_analytics.py              # Spark SQL
python integration/funnel.py               # integrated funnel analysis
python viz/charts.py                       # figures
```

### Troubleshooting
| Problem | Fix |
|---|---|
| `failed to connect to the docker API` | Start Docker Desktop and wait for the engine to run |
| `happybase` connects but cannot read | Add `127.0.0.1 hbase` to the hosts file; check Thrift on port 9090 |
| Spark `OutOfMemoryError` | Use `to_jsonl.py` first, keep `local[2]`, lower partition sizes |
| `git push` rejected (large files) | `data/` and `venv/` must be in `.gitignore` before the first commit |

## 10. Repository layout

```
generator/    dataset generator (reduced volumes, subcategory_id added)
mongodb/      load.py, pipelines.py
hbase/        schema.hbase, load.py, queries.py
spark/        to_jsonl.py, clean.py, recommend.py, sql_analytics.py
integration/  funnel.py
viz/          charts.py
results/      CSV outputs (funnel, product pairs)
report/       figures/ (charts used in the report)
Screenshots/  execution proofs (generation, loads, queries, Spark, funnel)
docker-compose.yml
```