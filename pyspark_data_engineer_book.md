# PySpark Interview Revision Handbook
## Complete Data Engineering Quick Revision Guide

> **Audience:** Azure Data Engineer • Databricks Engineer • Spark Developer • Production Data Engineer  
> **Format:** 20–30 page printable Markdown cheat book • Code-heavy • Interview-focused

---

## 🧭 How to Use This Handbook

| Icon | Meaning | Interview Usage |
|---|---|---|
| 🧠 | Concept | Define in 20 seconds |
| 🏭 | Real-world use case | Connect to production |
| 🐍 | PySpark | Code answer |
| 🧾 | SQL | SQL equivalent |
| ⚠️ | Interview Trap | Common mistake |
| 🚀 | Performance Tip | Tuning point |

> **Assumed imports used throughout**

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
```

---

# SECTION 1 — ⚙️ Spark Theory — Max 5 Pages

## 1.1 Spark in 60 Seconds

| Topic | Interview Answer |
|---|---|
| What is Spark? | Distributed compute engine for batch, streaming, SQL, ML-style processing on large data. |
| Why PySpark? | Python API over Spark JVM engine; ideal for ETL, lakehouse, Databricks, Azure Data Engineering. |
| Core abstraction | DataFrame/Dataset with Catalyst optimization; RDD is low-level. |
| Best fit | Large-scale transformations, joins, aggregations, streaming, Delta Lake pipelines. |

🏭 **Real-World Use Case:** Process daily sales files from ADLS/S3 into Delta Lake bronze/silver/gold tables.  
⚠️ **Interview Trap:** Spark is not a database; it is a compute engine. Storage is external: Delta/Parquet/ADLS/S3.  
🚀 **Performance Tip:** Prefer DataFrame/Spark SQL over Python UDFs because Catalyst can optimize expressions.

## 1.2 Architecture Diagram

```text
Client / Notebook / Job
        |
        v
+-------------------+        requests executors        +------------------+
| Driver Program    | -------------------------------> | Cluster Manager  |
| SparkSession      |                                  | YARN/K8s/DBR     |
| DAG Scheduler     | <------------------------------- |                  |
| Task Scheduler    |        executor allocation       +------------------+
+---------+---------+
          |
          | tasks
          v
+-------------------+     +-------------------+     +-------------------+
| Executor          |     | Executor          |     | Executor          |
| Cores + Memory    |     | Cores + Memory    |     | Cores + Memory    |
| Cache + Tasks     |     | Cache + Tasks     |     | Cache + Tasks     |
+-------------------+     +-------------------+     +-------------------+
```

| Component | Responsibility | Interview Tip |
|---|---|---|
| Driver | Builds logical/physical plan, schedules tasks, holds SparkSession | Do not collect huge data to driver. |
| Executor | Runs tasks and stores cached partitions | Executor failure can recompute from lineage. |
| Cluster Manager | Allocates resources | In Databricks, runtime manages Spark + optimized libraries. |

## 1.3 DAG, Lazy Evaluation, Transformations

| Concept | Short Definition | Example |
|---|---|---|
| DAG | Directed acyclic graph of operations | `read -> filter -> join -> write` |
| Lazy evaluation | Transformations execute only when action is called | `count`, `show`, `write` |
| Narrow transformation | No shuffle; one input partition to one output partition | `filter`, `select`, `map` |
| Wide transformation | Shuffle; data moves across partitions | `groupBy`, `join`, `distinct` |

🐍 **PySpark**
```python
df = spark.read.parquet('/mnt/bronze/orders')       # lazy
df2 = df.filter('amount > 0').select('order_id')   # lazy
df2.count()                                        # action triggers DAG
```

🧾 **SQL**
```sql
SELECT count(*) FROM parquet.`/mnt/bronze/orders` WHERE amount > 0;
```

⚠️ **Interview Trap:** `withColumn`, `filter`, `select` are not actions. `show()` is an action.  
🚀 **Performance Tip:** Chain transformations; Spark optimizes the full plan before execution.

## 1.4 Shuffle, Partitioning, Memory

| Area | Must-Know Answer |
|---|---|
| Partitioning | Splits data for parallelism; too few = underutilization, too many = overhead. |
| Shuffle | Expensive network/disk exchange caused by wide operations. |
| Memory | Execution memory for joins/sorts/shuffles; storage memory for cache. |
| Spill | Data exceeds memory and writes to disk; often caused by skew or large shuffle. |

🐍 **Check partitions**
```python
df.rdd.getNumPartitions()
df.repartition(200, 'customer_id')   # full shuffle
df.coalesce(20)                      # reduce partitions, usually no full shuffle
```

🧾 **SQL**
```sql
SET spark.sql.shuffle.partitions = 200;
```

⚠️ **Interview Trap:** Repartition increases/decreases with shuffle; coalesce usually only decreases.  
🚀 **Performance Tip:** Partition tables by low/medium-cardinality query filters like `event_date`, not unique IDs.

## 1.5 Catalyst, Tungsten, AQE

| Optimizer | What It Does | Interview Sound Bite |
|---|---|---|
| Catalyst | Logical + physical query optimization | Pushes filters, prunes columns, reorders operations. |
| Tungsten | Binary memory format + code generation | Reduces JVM object overhead. |
| AQE | Runtime plan optimization | Coalesces shuffle partitions, handles skew, changes join strategy. |

🐍 **Enable AQE**
```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')
```

🧾 **SQL**
```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
```

⚠️ **Interview Trap:** AQE helps, but it does not fix bad data modeling or unnecessary shuffles.  
🚀 **Performance Tip:** Use `explain('formatted')` to verify pushdown, join type, exchanges, and scans.

## 1.6 Core Lakehouse Concepts

| Concept | Short Interview Definition |
|---|---|
| Broadcast join | Sends small table to executors to avoid shuffling large table. |
| Cache/Persist | Materializes reused DataFrame partitions; unpersist when done. |
| Delta Lake | Transactional storage layer over data lake files with ACID, schema enforcement, time travel. |
| Batch vs Streaming | Batch handles bounded data; streaming handles unbounded incremental data. |
| Medallion | Bronze raw, Silver cleansed/conformed, Gold aggregated/serving. |
| Databricks Runtime | Optimized Spark runtime with Delta, Photon, connectors, libraries. |
| Unity Catalog | Central governance layer for catalogs, schemas, tables, volumes, permissions, lineage. |

```text
Raw Sources -> Bronze Delta -> Silver Delta -> Gold Delta -> BI / ML / APIs
              append/raw       clean/dedupe     aggregate/business
```

## 1.7 Architecture Interview Questions

| Question | Best Short Answer |
|---|---|
| Why is Spark lazy? | To optimize the whole DAG before execution. |
| Why is shuffle expensive? | Network + serialization + disk spill + stage boundary. |
| When broadcast? | Small dimension table, enough executor memory, avoid large shuffle. |
| Cache vs checkpoint? | Cache speeds reuse; checkpoint truncates lineage and writes reliable state. |
| Why Delta over Parquet? | ACID, schema enforcement/evolution, MERGE, time travel, optimized metadata. |

---

# SECTION 2 — 🧱 PySpark Essentials

🏭 **Real-World Use Case:** Start every Databricks/Azure ETL job with a reproducible SparkSession, schema, and temp views for validation.  
⚠️ **Interview Trap:** Explicit schemas avoid slow inference and inconsistent production types.  
🚀 **Performance Tip:** Avoid `collect()` except for tiny control metadata.

## 2.1 SparkSession + DataFrame Creation

| Topic | PySpark | SQL Equivalent / Note |
|---|---|---|
| SparkSession | `spark = SparkSession.builder.appName('etl').getOrCreate()` | Databricks notebooks already expose `spark`. |
| List to DF | `spark.createDataFrame([(1,'A')], ['id','name'])` | `SELECT 1 AS id, 'A' AS name` |
| Row objects | `spark.createDataFrame([Row(id=1, name='A')])` | Named fields from Row. |
| Temp view | `df.createOrReplaceTempView('orders_v')` | Session-scoped view. |
| Global temp | `df.createOrReplaceGlobalTempView('orders_g')` | Query as `global_temp.orders_g`. |

🐍 **Production schema**
```python
schema = StructType([
    StructField('order_id', StringType(), False),
    StructField('customer_id', StringType(), True),
    StructField('amount', DecimalType(12, 2), True),
    StructField('event_ts', TimestampType(), True)
])
orders = spark.createDataFrame([], schema)
orders.createOrReplaceTempView('orders_v')
```

🧾 **SQL**
```sql
CREATE OR REPLACE TEMP VIEW orders_v AS
SELECT cast(NULL AS STRING) order_id, cast(NULL AS DECIMAL(12,2)) amount WHERE 1=0;
```

💬 **Common Interview Questions**
- Why explicit schema? Faster, safer, prevents wrong types from corrupting silver tables.
- Temp vs global temp view? Temp is session-scoped; global temp is application-scoped under `global_temp`.

---

# SECTION 3 — 📥 Reading Data

🏭 **Real-World Use Case:** Land raw files from source systems into Bronze Delta with schema audit columns.  
⚠️ **Interview Trap:** `inferSchema=true` scans data and may infer different types across files.  
🚀 **Performance Tip:** Prefer Parquet/Delta for analytical reads; use CSV/JSON mostly at ingestion boundary.

## 3.1 Read Options Cheat Table

| Format | PySpark Reader | Key Options | SQL Equivalent |
|---|---|---|---|
| CSV | `spark.read.csv(path)` | `header`, `delimiter`, `mode`, `schema`, `multiLine`, `escape` | `read_files`/external table |
| JSON | `spark.read.json(path)` | `multiLine`, `mode`, `schema`, `primitivesAsString` | `SELECT * FROM json.`path`` in notebooks/path SQL |
| Parquet | `spark.read.parquet(path)` | `mergeSchema`, `recursiveFileLookup` | `parquet.`path`` |
| Delta | `spark.read.format('delta').load(path)` | `versionAsOf`, `timestampAsOf` | `delta.`path`` |
| Avro | `spark.read.format('avro')` | schema registry patterns | `USING avro` |
| ORC | `spark.read.orc(path)` | predicate pushdown | `USING orc` |
| JDBC | `spark.read.format('jdbc')` | `url`, `dbtable`, `partitionColumn` | external ingestion |
| Streaming | `spark.readStream...` | `cloudFiles`, `maxFilesPerTrigger` | streaming table/DLT |

## 3.2 Complete Read Examples

🐍 **CSV with explicit schema**
```python
orders_schema = 'order_id STRING, customer_id STRING, amount DECIMAL(12,2), event_ts TIMESTAMP'
orders_csv = (spark.read
  .option('header', 'true')
  .option('delimiter', ',')
  .option('mode', 'PERMISSIVE')
  .option('multiLine', 'false')
  .schema(orders_schema)
  .csv('/mnt/raw/orders/*.csv'))
```

🧾 **SQL**
```sql
CREATE TEMP VIEW orders_csv
USING csv
OPTIONS (path '/mnt/raw/orders/*.csv', header 'true', delimiter ',', mode 'PERMISSIVE');
```

🐍 **JSON, Parquet, Delta, Avro, ORC**
```python
json_df = spark.read.option('multiLine','true').schema(schema).json('/mnt/raw/events/')
parq_df = spark.read.option('recursiveFileLookup','true').parquet('/mnt/lake/parquet/orders')
delta_df = spark.read.format('delta').option('versionAsOf', 7).load('/mnt/delta/orders')
avro_df = spark.read.format('avro').load('/mnt/raw/avro/customers')
orc_df = spark.read.orc('/mnt/raw/orc/inventory')
```

🧾 **SQL**
```sql
SELECT * FROM delta.`/mnt/delta/orders` VERSION AS OF 7;
SELECT * FROM parquet.`/mnt/lake/parquet/orders`;
```

🐍 **JDBC parallel read**
```python
jdbc_df = (spark.read.format('jdbc')
  .option('url', jdbc_url)
  .option('dbtable', '(SELECT id, updated_at, amount FROM dbo.orders) q')
  .option('user', db_user).option('password', db_pwd)
  .option('partitionColumn', 'id').option('lowerBound', 1).option('upperBound', 10000000)
  .option('numPartitions', 32).load())
```

🐍 **Databricks Auto Loader / streaming read**
```python
bronze_stream = (spark.readStream.format('cloudFiles')
  .option('cloudFiles.format', 'csv')
  .option('cloudFiles.schemaLocation', '/mnt/checkpoints/orders_schema')
  .option('cloudFiles.inferColumnTypes', 'true')
  .option('cloudFiles.schemaEvolutionMode', 'rescue')
  .option('rescuedDataColumn', '_rescued_data')
  .load('/mnt/landing/orders'))
```

💬 **Interview Questions**
- CSV mode values? `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`.
- `mergeSchema`? Reads evolved Parquet/Delta schemas; useful but can slow metadata planning.
- `recursiveFileLookup`? Reads nested files but ignores partition discovery.

---

# SECTION 4 — 📤 Writing Data

🏭 **Real-World Use Case:** Write curated Silver/Gold Delta tables with partitioning and schema evolution.  
⚠️ **Interview Trap:** `overwrite` can delete data; use partition overwrite carefully.  
🚀 **Performance Tip:** Target ~128MB–1GB file sizes depending on workload; avoid thousands of tiny files.

## 4.1 Save Modes + Formats

| Mode | Meaning | Use Carefully |
|---|---|---|
| append | Add new files/rows | Standard ingestion. |
| overwrite | Replace target/table or partition | Dangerous without `replaceWhere`. |
| ignore | Do nothing if exists | Idempotent initial loads. |
| error/errorifexists | Fail if exists | Default safety. |

🐍 **Writes**
```python
(df.write.mode('append').format('delta').partitionBy('event_date').saveAsTable('main.sales.silver_orders'))
df.coalesce(1).write.mode('overwrite').option('header','true').csv('/mnt/export/orders_small')
df.write.mode('append').json('/mnt/export/json/orders')
df.write.mode('overwrite').parquet('/mnt/curated/parquet/orders')
```

🧾 **SQL**
```sql
CREATE TABLE main.sales.silver_orders USING DELTA PARTITIONED BY (event_date) AS SELECT * FROM orders_v;
INSERT INTO main.sales.silver_orders SELECT * FROM new_orders;
```

## 4.2 Advanced Write Patterns

🐍 **Dynamic partition overwrite + schema evolution**
```python
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
(df.write.format('delta')
  .mode('overwrite')
  .option('mergeSchema', 'true')
  .partitionBy('event_date')
  .saveAsTable('main.sales.silver_orders'))
```

🐍 **Bucket and sort for repeated joins**
```python
(df.write.mode('overwrite')
  .bucketBy(64, 'customer_id')
  .sortBy('customer_id')
  .saveAsTable('main.sales.bucketed_orders'))
```

🧾 **SQL**
```sql
INSERT OVERWRITE main.sales.silver_orders
SELECT * FROM stage_orders WHERE event_date = current_date();
```

💬 **Best Practices**
- Use `repartition('event_date')` before high-volume partitioned writes.
- Use `coalesce(n)` only when reducing output files after filtering.
- Use Delta `OPTIMIZE` for compaction instead of forcing `coalesce(1)`.

---

# SECTION 5 — 🔄 DataFrame Transformations

🏭 **Real-World Use Case:** Convert raw bronze records into standardized silver columns.  
⚠️ **Interview Trap:** Repeated `withColumn` can create long plans; prefer one `select` when deriving many columns.  
🚀 **Performance Tip:** Built-in functions are optimized; Python UDFs often block optimization.

## 5.1 Transformation Matrix

| Function | PySpark Example | SQL Equivalent | Interview Tip |
|---|---|---|---|
| select | `df.select('id','amount')` | `SELECT id, amount` | Column pruning. |
| alias | `F.col('amount').alias('net')` | `amount AS net` | Use for joins. |
| withColumn | `df.withColumn('tax', F.col('amount')*.1)` | `amount*.1 AS tax` | Avoid loops. |
| drop | `df.drop('raw_payload')` | omit column | Reduces width. |
| rename | `df.withColumnRenamed('dt','event_date')` | `dt AS event_date` | Normalize names. |
| cast | `F.col('id').cast('long')` | `CAST(id AS BIGINT)` | Fix types early. |
| expr | `F.expr('amount * 1.1')` | same expression | Great for SQL snippets. |
| when | `F.when(F.col('amount')>0,'Y').otherwise('N')` | `CASE WHEN` | Null-aware logic. |
| lit | `F.lit('ERP')` | `'ERP'` | Add constants. |
| explode | `df.select(F.explode('items'))` | `LATERAL VIEW explode` | Flattens arrays. |
| split | `F.split('tags', ',')` | `split(tags, ',')` | String to array. |
| regexp_replace | `F.regexp_replace('phone','[^0-9]','')` | same | Clean strings. |
| concat_ws | `F.concat_ws('|','id','dt')` | `concat_ws('|', id, dt)` | Build keys. |
| pivot | `df.groupBy('id').pivot('status').count()` | `PIVOT` | Specify values for speed. |
| stack/unpivot | `selectExpr("id", "stack(2,'a',a,'b',b) as (metric,val)")` | `stack` | Wide-to-long. |
| unionByName | `a.unionByName(b, allowMissingColumns=True)` | `UNION ALL` aligned | Safer than position union. |
| distinct | `df.distinct()` | `SELECT DISTINCT` | Causes shuffle. |

🐍 **Production select pattern**
```python
silver = bronze.select(
    F.col('order_id').cast('string'),
    F.col('customer_id').cast('string'),
    F.round('amount', 2).alias('amount'),
    F.to_date('event_ts').alias('event_date'),
    F.when(F.col('amount') >= 1000, 'HIGH').otherwise('NORMAL').alias('order_band'),
    F.current_timestamp().alias('processed_ts')
)
```

🧾 **SQL**
```sql
SELECT cast(order_id AS STRING), round(amount,2) amount,
       to_date(event_ts) event_date,
       CASE WHEN amount >= 1000 THEN 'HIGH' ELSE 'NORMAL' END order_band,
       current_timestamp() processed_ts
FROM bronze_orders;
```

---

# SECTION 6 — 🔎 Filtering & Conditional Logic

🏭 **Real-World Use Case:** Keep only valid, recent, business-approved records for Silver.  
⚠️ **Interview Trap:** Use parentheses with `&` and `|`; Python operator precedence can break filters.  
🚀 **Performance Tip:** Filter early to reduce shuffle volume; partition filters enable pruning.

🐍 **PySpark patterns**
```python
valid = (df
  .filter((F.col('amount') > 0) & F.col('status').isin('PAID', 'SHIPPED'))
  .where(F.col('event_date').between('2026-01-01', '2026-12-31'))
  .filter(F.col('email').like('%@%'))
  .filter(F.col('sku').rlike('^[A-Z]{3}-[0-9]{4}$'))
  .withColumn('priority', F.when(F.col('amount') >= 1000, 'P1').otherwise('P2')))
```

🧾 **SQL**
```sql
SELECT *, CASE WHEN amount >= 1000 THEN 'P1' ELSE 'P2' END priority
FROM orders
WHERE amount > 0
  AND status IN ('PAID','SHIPPED')
  AND event_date BETWEEN DATE '2026-01-01' AND DATE '2026-12-31'
  AND email LIKE '%@%'
  AND sku RLIKE '^[A-Z]{3}-[0-9]{4}$';
```

💬 **Interview Quick Answer:** `filter` and `where` are aliases in DataFrame API.

---

# SECTION 7 — 🤝 Joins

🏭 **Real-World Use Case:** Enrich fact orders with customer/product dimensions.  
⚠️ **Interview Trap:** Inner join drops unmatched rows; left semi returns only left columns; left anti finds missing rows.  
🚀 **Performance Tip:** Broadcast small dimensions; handle skewed keys before joining.

## 7.1 Join Types

| Join | PySpark | SQL | Use Case |
|---|---|---|---|
| inner | `orders.join(cust,'customer_id','inner')` | `INNER JOIN` | Matching records. |
| left | `how='left'` | `LEFT JOIN` | Preserve facts. |
| right | `how='right'` | `RIGHT JOIN` | Rare; prefer left by swapping. |
| full | `how='full'` | `FULL OUTER JOIN` | Reconciliation. |
| semi | `how='left_semi'` | `WHERE EXISTS` | Filter left by existence. |
| anti | `how='left_anti'` | `WHERE NOT EXISTS` | Orphans/missing dimensions. |
| self | `a.join(b, ...)` | self join | Hierarchies. |

🐍 **Code examples**
```python
joined = orders.alias('o').join(customers.alias('c'), 'customer_id', 'left')
missing_dim = orders.join(customers, 'customer_id', 'left_anti')
active_orders = orders.join(active_customers.select('customer_id'), 'customer_id', 'left_semi')
```

🧾 **SQL**
```sql
SELECT o.*, c.segment FROM orders o LEFT JOIN customers c USING (customer_id);
SELECT o.* FROM orders o LEFT ANTI JOIN customers c USING (customer_id);
SELECT o.* FROM orders o LEFT SEMI JOIN active_customers c USING (customer_id);
```

## 7.2 Broadcast + Skew Handling

🐍 **Broadcast join**
```python
fast = orders.join(F.broadcast(customers), 'customer_id', 'left')
```

🧾 **SQL**
```sql
SELECT /*+ BROADCAST(c) */ o.*, c.segment
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id;
```

🐍 **Skew salting pattern**
```python
salted_orders = orders.withColumn('salt', (F.rand()*10).cast('int'))
salted_dim = customers.crossJoin(spark.range(10).withColumnRenamed('id','salt'))
fixed = salted_orders.join(salted_dim, ['customer_id','salt'], 'left').drop('salt')
```

💬 **Real Interview Problem:** Find orders without customer dimension → use `left_anti`.

---

# SECTION 8 — 📊 Aggregations

🏭 **Real-World Use Case:** Build daily revenue and customer KPI Gold tables.  
⚠️ **Interview Trap:** `collect_list` order is not guaranteed after shuffle unless explicitly sorted.  
🚀 **Performance Tip:** Aggregate after filtering and column pruning.

🐍 **Aggregation patterns**
```python
daily = (orders.groupBy('event_date', 'region')
  .agg(F.count('*').alias('orders'),
       F.sum('amount').alias('revenue'),
       F.approx_count_distinct('customer_id').alias('approx_customers'),
       F.collect_set('status').alias('statuses'),
       F.collect_list('order_id').alias('order_ids')))

roll = orders.rollup('region','event_date').agg(F.sum('amount').alias('revenue'))
cube = orders.cube('region','channel').agg(F.sum('amount').alias('revenue'))
piv = orders.groupBy('region').pivot('status', ['PAID','REFUND']).agg(F.sum('amount'))
```

🧾 **SQL**
```sql
SELECT event_date, region, count(*) orders, sum(amount) revenue,
       approx_count_distinct(customer_id) approx_customers,
       collect_set(status) statuses
FROM orders GROUP BY event_date, region;

SELECT region, event_date, sum(amount) FROM orders GROUP BY ROLLUP(region, event_date);
SELECT * FROM orders PIVOT (sum(amount) FOR status IN ('PAID','REFUND'));
```

💬 **Interview Tip:** `approx_count_distinct` is faster than exact `countDistinct` for large cardinality.

---

# SECTION 9 — 🪟 Window Functions

🏭 **Real-World Use Case:** Deduplicate by latest event, calculate running customer spend, find top products.  
⚠️ **Interview Trap:** Window without partition can move all data to one partition.  
🚀 **Performance Tip:** Partition windows by business key and order by deterministic timestamp + tie breaker.

🐍 **Window patterns**
```python
w_latest = Window.partitionBy('order_id').orderBy(F.col('event_ts').desc(), F.col('ingest_ts').desc())
w_cust = Window.partitionBy('customer_id').orderBy('event_ts')
w_mov = w_cust.rowsBetween(-6, 0)

out = (orders
  .withColumn('rn', F.row_number().over(w_latest))
  .withColumn('rnk', F.rank().over(Window.partitionBy('region').orderBy(F.col('amount').desc())))
  .withColumn('dense_rnk', F.dense_rank().over(Window.partitionBy('region').orderBy(F.col('amount').desc())))
  .withColumn('prev_amount', F.lag('amount').over(w_cust))
  .withColumn('next_amount', F.lead('amount').over(w_cust))
  .withColumn('running_total', F.sum('amount').over(w_cust.rowsBetween(Window.unboundedPreceding, 0)))
  .withColumn('moving_avg_7', F.avg('amount').over(w_mov))
  .withColumn('first_order', F.first('event_ts').over(w_cust))
  .withColumn('last_order', F.last('event_ts').over(w_cust.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))))

latest = out.filter('rn = 1').drop('rn')
top3 = out.filter('dense_rnk <= 3')
```

🧾 **SQL**
```sql
SELECT * FROM (
  SELECT o.*, row_number() OVER(PARTITION BY order_id ORDER BY event_ts DESC, ingest_ts DESC) rn,
         sum(amount) OVER(PARTITION BY customer_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_total
  FROM orders o
) WHERE rn = 1;
```

💬 **Interview Coding Patterns:** latest record = `row_number=1`; top N with ties = `dense_rank <= N`; strict top N = `row_number <= N`.

---

# SECTION 10 — 🧹 Null & Duplicate Handling

🏭 **Real-World Use Case:** Clean malformed bronze data before silver load.  
⚠️ **Interview Trap:** `dropDuplicates(['id'])` keeps arbitrary row; use window for deterministic latest.  
🚀 **Performance Tip:** Dedup on minimal keys; full-row distinct causes wide shuffle.

🐍 **Null and duplicate code**
```python
clean = (df
  .dropna(subset=['order_id'])
  .fillna({'status': 'UNKNOWN', 'amount': 0})
  .replace(['N/A', 'NULL', ''], None, subset=['email'])
  .withColumn('safe_amount', F.coalesce('amount', F.lit(0))))

# deterministic dedup
w = Window.partitionBy('order_id').orderBy(F.col('event_ts').desc())
dedup = clean.withColumn('rn', F.row_number().over(w)).filter('rn=1').drop('rn')

# null-safe join
matched = a.join(b, a.key.eqNullSafe(b.key), 'inner')
```

🧾 **SQL**
```sql
SELECT coalesce(amount, 0) safe_amount FROM orders;
SELECT * FROM a JOIN b ON a.key <=> b.key;
```

---

# SECTION 11 — 🔤 String Functions

🏭 **Real-World Use Case:** Standardize names, emails, SKUs, phone numbers.  
⚠️ **Interview Trap:** Regex can be expensive; avoid repeated regex on huge datasets if simple functions work.  
🚀 **Performance Tip:** Normalize join keys before joining and persist if reused.

🐍 **String function pack**
```python
std = df.select(
  F.trim('name').alias('name_trim'),
  F.upper('country').alias('country'),
  F.lower('email').alias('email'),
  F.substring('sku', 1, 3).alias('sku_prefix'),
  F.regexp_extract('email', r'@(.+)$', 1).alias('domain'),
  F.regexp_replace('phone', r'[^0-9]', '').alias('phone_digits'),
  F.translate('text', 'áéíóú', 'aeiou').alias('ascii_text'),
  F.concat(F.col('first'), F.lit(' '), F.col('last')).alias('full_name'),
  F.split('tags', ',').alias('tag_array'))
```

🧾 **SQL**
```sql
SELECT trim(name), upper(country), lower(email), substring(sku,1,3),
       regexp_extract(email, '@(.+)$', 1), regexp_replace(phone, '[^0-9]', ''),
       translate(text, 'áéíóú', 'aeiou'), concat(first, ' ', last), split(tags, ',')
FROM customers;
```

---

# SECTION 12 — 🕒 Date & Timestamp Functions

🏭 **Real-World Use Case:** Build date partitions, SLA aging, time-zone-safe event pipelines.  
⚠️ **Interview Trap:** Store timestamps in UTC; convert only for presentation/business rules.  
🚀 **Performance Tip:** Partition by derived `event_date`, not raw timestamp.

🐍 **Date/time pack**
```python
dated = df.select(
  F.current_date().alias('load_date'),
  F.current_timestamp().alias('load_ts'),
  F.datediff(F.current_date(), F.to_date('event_ts')).alias('age_days'),
  F.date_add(F.to_date('event_ts'), 7).alias('plus_7_days'),
  F.add_months(F.to_date('event_ts'), 1).alias('next_month'),
  F.trunc(F.to_date('event_ts'), 'MM').alias('month_start'),
  F.from_utc_timestamp('event_ts', 'America/New_York').alias('event_ts_est'),
  F.to_utc_timestamp('local_ts', 'America/New_York').alias('event_ts_utc'))
```

🐍 **Streaming window**
```python
agg = (stream_df.withWatermark('event_ts', '10 minutes')
  .groupBy(F.window('event_ts', '5 minutes'), 'device_id')
  .agg(F.count('*').alias('events')))
```

🧾 **SQL**
```sql
SELECT current_date(), current_timestamp(), datediff(current_date(), to_date(event_ts)),
       date_add(to_date(event_ts), 7), add_months(to_date(event_ts), 1), trunc(to_date(event_ts), 'MM'),
       from_utc_timestamp(event_ts, 'America/New_York')
FROM events;
```

---

# SECTION 13 — 🧩 Complex Types

🏭 **Real-World Use Case:** Flatten nested JSON events from APIs/Kafka into relational silver tables.  
⚠️ **Interview Trap:** Explode multiplies rows; understand cardinality before exploding.  
🚀 **Performance Tip:** Parse JSON once with schema using `from_json`; avoid repeated `get_json_object`.

🐍 **Arrays, structs, maps, JSON**
```python
json_schema = StructType([
  StructField('user', StructType([StructField('id', StringType()), StructField('tier', StringType())])),
  StructField('items', ArrayType(StructType([StructField('sku', StringType()), StructField('qty', IntegerType())]))),
  StructField('attrs', MapType(StringType(), StringType()))
])

parsed = raw.select(F.from_json('payload', json_schema).alias('p'))
flat = parsed.select('p.user.id', 'p.user.tier', F.explode('p.items').alias('item'), 'p.attrs')
items = flat.select('id', 'tier', 'item.sku', 'item.qty', F.col('attrs')['source'].alias('source'))
posed = parsed.select(F.posexplode('p.items').alias('pos', 'item'))
inlined = parsed.select(F.inline('p.items'))
json_out = items.select(F.to_json(F.struct('*')).alias('payload'))
```

🧾 **SQL**
```sql
SELECT p.user.id, p.user.tier, item.sku, item.qty
FROM (SELECT from_json(payload, schema_of_json(payload)) p FROM raw) r
LATERAL VIEW explode(p.items) e AS item;
```

---

# SECTION 14 — 🚀 Performance Optimization

🏭 **Real-World Use Case:** Tune a slow nightly Gold aggregation from 3 hours to 20 minutes.  
⚠️ **Interview Trap:** Adding more executors does not fix skew, small files, or poor partitioning.  
🚀 **Performance Tip:** Measure with Spark UI + `explain`; tune the bottleneck, not guesses.

## 14.1 Optimization Decision Table

| Problem | Symptom | Fix |
|---|---|---|
| Too many small files | Slow listing/scans | Delta `OPTIMIZE`, Auto Optimize, compact writes. |
| Skew | One task runs forever | AQE skew join, salting, split hot keys. |
| Large shuffle | Slow joins/groupBy | Broadcast small tables, pre-aggregate, partition by key. |
| Reading too much | Slow scan | Predicate pushdown, column pruning, partition pruning. |
| Reused expensive DF | Recomputed stages | `cache/persist`, then action to materialize. |
| Too many partitions | Scheduler overhead | AQE coalesce, tune shuffle partitions. |
| Too few partitions | Low parallelism | `repartition`. |

🐍 **Performance code snippets**
```python
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.shuffle.partitions', 'auto')  # Databricks supports auto in many runtimes
spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
spark.conf.set('spark.databricks.delta.autoCompact.enabled', 'true')

narrow = df.select('event_date','customer_id','amount').filter(F.col('event_date') >= '2026-01-01')
reused = narrow.persist()
reused.count()
result = reused.groupBy('customer_id').agg(F.sum('amount').alias('revenue'))
reused.unpersist()
```

🧾 **SQL**
```sql
EXPLAIN FORMATTED SELECT customer_id, sum(amount) FROM orders WHERE event_date >= '2026-01-01' GROUP BY customer_id;
OPTIMIZE main.sales.silver_orders ZORDER BY (customer_id);
VACUUM main.sales.silver_orders RETAIN 168 HOURS;
```

## 14.2 Interview Tuning Checklist

1. Check file format: Delta/Parquet > CSV/JSON.
2. Verify filters push down and partition pruning happens.
3. Verify no unnecessary `distinct`, `orderBy`, full-row `groupBy`.
4. Broadcast dimensions when safe.
5. Find skewed keys: `df.groupBy(key).count().orderBy(desc('count'))`.
6. Compact small files and Z-order frequently filtered columns.
7. Avoid Python UDFs; use SQL functions or pandas UDF only when justified.
8. Right-size clusters and use Photon where available for SQL/DataFrame workloads.

---

# SECTION 15 — 🧬 Delta Lake

🏭 **Real-World Use Case:** Maintain ACID customer/order tables with upserts, deletes, auditability, and time travel.  
⚠️ **Interview Trap:** Delta schema evolution is controlled; schema enforcement prevents accidental bad writes.  
🚀 **Performance Tip:** Run `OPTIMIZE` after high-frequency small appends; use Z-order for selective filters.

🐍 **Create, update, delete, time travel**
```python
(df.write.format('delta').mode('overwrite')
  .option('overwriteSchema', 'true')
  .saveAsTable('main.sales.orders_delta'))

hist = spark.sql('DESCRIBE HISTORY main.sales.orders_delta')
old = spark.read.format('delta').option('versionAsOf', 3).table('main.sales.orders_delta')

DeltaTable.forName(spark, 'main.sales.orders_delta').delete("status = 'CANCELLED'")
DeltaTable.forName(spark, 'main.sales.orders_delta').update(
  condition="status = 'PENDING' AND event_date < current_date() - 30",
  set={'status': "'EXPIRED'"})
```

🧾 **SQL**
```sql
CREATE TABLE main.sales.orders_delta USING DELTA AS SELECT * FROM orders;
SELECT * FROM main.sales.orders_delta VERSION AS OF 3;
DELETE FROM main.sales.orders_delta WHERE status = 'CANCELLED';
UPDATE main.sales.orders_delta SET status = 'EXPIRED' WHERE status='PENDING';
```

## 15.1 MERGE / Upsert Interview Pattern

🐍 **PySpark Delta MERGE**
```python
tgt = DeltaTable.forName(spark, 'main.sales.silver_orders')
(tgt.alias('t').merge(updates.alias('s'), 't.order_id = s.order_id')
  .whenMatchedUpdate(condition='s.event_ts >= t.event_ts', set={
      'customer_id': 's.customer_id', 'amount': 's.amount', 'status': 's.status',
      'event_ts': 's.event_ts', 'updated_ts': 'current_timestamp()'})
  .whenNotMatchedInsert(values={
      'order_id': 's.order_id', 'customer_id': 's.customer_id', 'amount': 's.amount',
      'status': 's.status', 'event_ts': 's.event_ts', 'created_ts': 'current_timestamp()',
      'updated_ts': 'current_timestamp()'})
  .execute())
```

🧾 **SQL**
```sql
MERGE INTO main.sales.silver_orders t
USING updates s
ON t.order_id = s.order_id
WHEN MATCHED AND s.event_ts >= t.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

🐍 **Change Data Feed read**
```python
changes = (spark.read.format('delta')
  .option('readChangeFeed', 'true')
  .option('startingVersion', 10)
  .table('main.sales.silver_orders'))
```

---

# SECTION 16 — 🌊 Structured Streaming

🏭 **Real-World Use Case:** Continuously ingest transactions from cloud files/Kafka into Bronze and update Gold aggregates.  
⚠️ **Interview Trap:** Checkpoint path is mandatory for recovery and exactly-once sink semantics.  
🚀 **Performance Tip:** Use watermarks to bound state for late-arriving events.

🐍 **Stream read/write with checkpoint**
```python
stream = (spark.readStream.format('delta')
  .table('main.sales.bronze_orders'))

query = (stream.writeStream.format('delta')
  .option('checkpointLocation', '/mnt/checkpoints/silver_orders')
  .outputMode('append')
  .trigger(processingTime='1 minute')
  .toTable('main.sales.silver_orders_stream'))
```

🐍 **Watermark + window aggregation**
```python
agg = (stream.withWatermark('event_ts', '15 minutes')
  .groupBy(F.window('event_ts', '10 minutes'), 'region')
  .agg(F.sum('amount').alias('revenue')))

(agg.writeStream.format('delta')
  .outputMode('append')
  .option('checkpointLocation', '/mnt/checkpoints/gold_region_10m')
  .toTable('main.sales.gold_region_10m'))
```

🐍 **foreachBatch upsert**
```python
def upsert_orders(batch_df, batch_id):
    batch_df.createOrReplaceTempView('updates')
    spark.sql('''
      MERGE INTO main.sales.silver_orders t USING updates s ON t.order_id=s.order_id
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    ''')

(stream.writeStream.foreachBatch(upsert_orders)
  .option('checkpointLocation', '/mnt/checkpoints/orders_merge')
  .trigger(availableNow=True).start())
```

🧾 **SQL**
```sql
CREATE STREAMING TABLE silver_orders_stream AS SELECT * FROM STREAM(main.sales.bronze_orders);
```

💬 **Exactly Once:** Spark uses offsets/checkpoints plus idempotent transactional sinks like Delta.

---

# SECTION 17 — 📂 Databricks Auto Loader

🏭 **Real-World Use Case:** Incrementally ingest millions of files from ADLS/S3/GCS without re-listing everything.  
⚠️ **Interview Trap:** Schema location should be stable and separate per stream/source.  
🚀 **Performance Tip:** Use notifications/file events for large directories; directory listing is okay for smaller workloads.

🐍 **cloudFiles with schema evolution + rescue**
```python
auto = (spark.readStream.format('cloudFiles')
  .option('cloudFiles.format', 'json')
  .option('cloudFiles.schemaLocation', '/mnt/schema/orders_json')
  .option('cloudFiles.schemaEvolutionMode', 'rescue')
  .option('rescuedDataColumn', '_rescued_data')
  .option('cloudFiles.includeExistingFiles', 'true')
  .option('cloudFiles.useNotifications', 'true')
  .load('/mnt/landing/orders_json'))

(auto.withColumn('ingest_ts', F.current_timestamp())
  .writeStream.format('delta')
  .option('checkpointLocation', '/mnt/checkpoints/bronze_orders_json')
  .trigger(availableNow=True)
  .toTable('main.sales.bronze_orders_json'))
```

🧾 **SQL**
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders_json AS
SELECT *, current_timestamp() ingest_ts
FROM cloud_files('/mnt/landing/orders_json', 'json', map('cloudFiles.schemaEvolutionMode','rescue'));
```

💬 **Interview Scenario:** Auto Loader vs manual batch listing → Auto Loader tracks discovered files, supports schema evolution, and scales better for incremental ingestion.

---

# SECTION 18 — 📋 COPY INTO

🏭 **Real-World Use Case:** Simple idempotent incremental file loading from landing folder to Delta for batch pipelines.  
⚠️ **Interview Trap:** COPY INTO is not a continuous stream; Auto Loader is better for always-on ingestion.  
🚀 **Performance Tip:** Use COPY INTO for simple periodic loads; use Auto Loader for high scale and schema evolution.

🧾 **COPY INTO syntax**
```sql
COPY INTO main.sales.bronze_orders
FROM '/mnt/landing/orders'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='false', 'delimiter'=',')
COPY_OPTIONS ('mergeSchema'='true');
```

🐍 **Run from PySpark**
```python
spark.sql('''
COPY INTO main.sales.bronze_orders
FROM '/mnt/landing/orders'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'delimiter'=',')
COPY_OPTIONS ('mergeSchema'='true')
''')
```

| COPY INTO | Auto Loader |
|---|---|
| SQL-first batch incremental load | Streaming/incremental ingestion service |
| Simple setup | Best for huge file counts |
| Tracks loaded files | Schema inference/evolution + rescue |
| Scheduled jobs | Continuous or `availableNow` jobs |

---

# SECTION 19 — 🏗️ Delta Live Tables (DLT)

🏭 **Real-World Use Case:** Declarative medallion pipeline with data quality expectations and managed orchestration.  
⚠️ **Interview Trap:** DLT pipeline code defines tables; do not treat it like ad hoc notebook writes.  
🚀 **Performance Tip:** Use expectations to quarantine/drop bad data early and keep Silver reliable.

🐍 **DLT Python medallion**
```python
import dlt

@dlt.table(name='bronze_orders')
def bronze_orders():
    return (spark.readStream.format('cloudFiles')
      .option('cloudFiles.format','json')
      .load('/mnt/landing/orders'))

@dlt.table(name='silver_orders')
@dlt.expect_or_drop('valid_order_id', 'order_id IS NOT NULL')
@dlt.expect('positive_amount', 'amount >= 0')
def silver_orders():
    return dlt.read_stream('bronze_orders').selectExpr('cast(order_id as string) order_id', 'cast(amount as decimal(12,2)) amount', 'event_ts')

@dlt.table(name='gold_daily_sales')
def gold_daily_sales():
    return dlt.read('silver_orders').groupBy(F.to_date('event_ts').alias('event_date')).agg(F.sum('amount').alias('revenue'))
```

🧾 **DLT SQL**
```sql
CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders AS SELECT * FROM cloud_files('/mnt/landing/orders','json');
CREATE OR REFRESH LIVE TABLE gold_daily_sales AS SELECT to_date(event_ts) event_date, sum(amount) revenue FROM LIVE.silver_orders GROUP BY 1;
```

💬 **DLT Objects:** Streaming tables ingest continuously; materialized views maintain computed results; expectations enforce quality.

---

# SECTION 20 — 🧱 Databricks Objects

🏭 **Real-World Use Case:** Govern production lakehouse assets with Unity Catalog, external locations, secrets, and cluster policies.  
⚠️ **Interview Trap:** DBFS mounts are legacy in many governed environments; prefer Unity Catalog external locations and volumes.  
🚀 **Performance Tip:** Use managed Delta tables for simplicity unless cross-platform direct file access is required.

| Object | Meaning | Example |
|---|---|---|
| Catalog | Top governance namespace | `main` |
| Schema | Database inside catalog | `main.sales` |
| Managed table | Databricks manages data path | `CREATE TABLE main.sales.orders` |
| External table | Metadata points to external location | `LOCATION 'abfss://...'` |
| Volume | Governed files/object storage access | `main.raw.orders_vol` |
| Secret scope | Secure credentials | `dbutils.secrets.get` |
| Service principal | App identity for jobs/CI | Azure AD app/SPN |
| Cluster policy | Controls cluster settings/cost/security | Enforce runtime, node types |

🧾 **Unity Catalog SQL**
```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.sales;
CREATE VOLUME IF NOT EXISTS main.raw.landing;
GRANT SELECT ON TABLE main.sales.gold_daily_sales TO `data_analysts`;
CREATE TABLE main.sales.orders_ext USING DELTA LOCATION 'abfss://curated@acct.dfs.core.windows.net/orders';
```

🐍 **Secrets**
```python
jdbc_pwd = dbutils.secrets.get(scope='kv-prod', key='sql-password')
```

---

# SECTION 21 — 🛠️ End-to-End Pipeline: CSV → Bronze → Silver → Gold

🏭 **Real-World Use Case:** Daily retail order ingestion into governed Delta Lake tables.  
⚠️ **Interview Trap:** Do not deduplicate after aggregation; dedupe before Gold metrics.  
🚀 **Performance Tip:** Partition Bronze/Silver by event date and optimize Gold for BI query keys.

## 21.1 Batch Pipeline

🐍 **1) Ingestion to Bronze**
```python
raw_schema = 'order_id STRING, customer_id STRING, amount DECIMAL(12,2), status STRING, event_ts TIMESTAMP'
bronze = (spark.read.option('header','true').schema(raw_schema).csv('/Volumes/main/raw/landing/orders/*.csv')
  .withColumn('source_file', F.input_file_name())
  .withColumn('ingest_ts', F.current_timestamp())
  .withColumn('event_date', F.to_date('event_ts')))

(bronze.write.format('delta').mode('append').partitionBy('event_date').saveAsTable('main.sales.bronze_orders'))
```

🐍 **2) Cleanse + dedupe Silver**
```python
b = spark.table('main.sales.bronze_orders')
w = Window.partitionBy('order_id').orderBy(F.col('event_ts').desc(), F.col('ingest_ts').desc())
silver = (b.filter('order_id IS NOT NULL AND amount >= 0')
  .withColumn('rn', F.row_number().over(w)).filter('rn=1').drop('rn')
  .withColumn('status', F.upper(F.trim('status'))))

(silver.write.format('delta').mode('overwrite').option('mergeSchema','true')
  .partitionBy('event_date').saveAsTable('main.sales.silver_orders'))
```

🐍 **3) Gold aggregation**
```python
gold = (spark.table('main.sales.silver_orders')
  .groupBy('event_date', 'status')
  .agg(F.count('*').alias('order_count'), F.sum('amount').alias('revenue')))

gold.write.format('delta').mode('overwrite').saveAsTable('main.sales.gold_daily_orders')
spark.sql('OPTIMIZE main.sales.gold_daily_orders ZORDER BY (event_date, status)')
```

🧾 **SQL equivalent**
```sql
CREATE OR REPLACE TABLE main.sales.gold_daily_orders AS
SELECT event_date, status, count(*) order_count, sum(amount) revenue
FROM main.sales.silver_orders GROUP BY event_date, status;
```

## 21.2 Streaming Version with Merge

🐍 **Auto Loader Bronze + foreachBatch Silver MERGE**
```python
stream_bronze = (spark.readStream.format('cloudFiles')
  .option('cloudFiles.format','csv')
  .option('header','true')
  .option('cloudFiles.schemaLocation','/mnt/schema/orders_csv')
  .load('/Volumes/main/raw/landing/orders'))

(stream_bronze.withColumn('ingest_ts', F.current_timestamp())
  .writeStream.format('delta')
  .option('checkpointLocation','/mnt/checkpoints/bronze_orders')
  .trigger(availableNow=True)
  .toTable('main.sales.bronze_orders_stream'))
```

---

# SECTION 22 — 💻 Interview Coding Patterns

🏭 **Real-World Use Case:** Solve common hands-on rounds quickly with reusable templates.  
⚠️ **Interview Trap:** Always mention tie-breakers and null handling.  
🚀 **Performance Tip:** Choose window vs aggregation based on required output columns.

| Problem | PySpark Pattern | SQL Pattern |
|---|---|---|
| Top N per group | `dense_rank().over(partitionBy(g).orderBy(desc(metric))) <= N` | `DENSE_RANK() OVER...` |
| Latest record | `row_number` over key order by timestamp desc | `ROW_NUMBER()... WHERE rn=1` |
| Duplicate removal | window deterministic dedupe | CTE with row_number |
| Running totals | `sum().over(rowsBetween(unboundedPreceding,0))` | window sum |
| Gap analysis | `datediff(date, lag(date))` | `LAG` |
| SCD Type 2 | expire current + insert new version | `MERGE` with current flag |
| CDC merge | route I/U/D operation codes | `MERGE WHEN MATCHED DELETE/UPDATE` |
| Sessionization | gap > 30 min creates new session id | lag + cumulative sum |
| Incremental load | watermark/high-watermark filter | `WHERE updated_at > last_max` |

🐍 **Gap analysis + sessionization**
```python
w = Window.partitionBy('user_id').orderBy('event_ts')
sessions = (events
  .withColumn('prev_ts', F.lag('event_ts').over(w))
  .withColumn('gap_min', (F.col('event_ts').cast('long') - F.col('prev_ts').cast('long'))/60)
  .withColumn('new_session', F.when(F.col('prev_ts').isNull() | (F.col('gap_min') > 30), 1).otherwise(0))
  .withColumn('session_num', F.sum('new_session').over(w.rowsBetween(Window.unboundedPreceding, 0))))
```

🐍 **CDC merge with deletes**
```python
DeltaTable.forName(spark, 'main.sales.dim_customer').alias('t').merge(
  cdc.alias('s'), 't.customer_id=s.customer_id') \
  .whenMatchedDelete(condition="s.op = 'D'") \
  .whenMatchedUpdate(condition="s.op in ('U','I')", set={'name':'s.name','updated_ts':'s.updated_ts'}) \
  .whenNotMatchedInsert(condition="s.op in ('I','U')", values={'customer_id':'s.customer_id','name':'s.name','updated_ts':'s.updated_ts'}) \
  .execute()
```

🧾 **SCD Type 2 SQL sketch**
```sql
MERGE INTO dim_customer t USING staged_changes s
ON t.customer_id=s.customer_id AND t.is_current=true
WHEN MATCHED AND t.hash <> s.hash THEN UPDATE SET is_current=false, end_date=s.effective_date;
-- Then INSERT new current rows from staged_changes not currently active.
```

---

# SECTION 23 — 🎯 Common Interview Questions

🏭 **Real-World Use Case:** Prepare crisp 30–60 second answers for architecture and production support rounds.  
⚠️ **Interview Trap:** Do not answer every performance issue with “increase cluster size.”  
🚀 **Performance Tip:** Explain how you would prove the bottleneck with Spark UI, query plan, and table/file metrics.

## 23.1 Most Asked Questions

| Category | Question | High-Impact Answer |
|---|---|---|
| Architecture | Driver vs executor? | Driver plans/schedules; executors run tasks/cache partitions. |
| Architecture | Lazy evaluation? | Builds optimized DAG and executes only on action. |
| Shuffle | Why slow? | Network/disk/serialization/stage boundary; reduce with broadcast/pre-aggregation. |
| Joins | Broadcast join? | Replicate small table to executors to avoid shuffling large table. |
| Skew | How detect/fix? | Long tail tasks, key counts; AQE, salting, split hot keys. |
| Delta | Why Delta? | ACID, MERGE, time travel, schema enforcement, optimized metadata. |
| Streaming | Exactly once? | Checkpoints + source offsets + transactional sink/idempotent writes. |
| Databricks | Unity Catalog? | Central governance for data/AI assets, permissions, lineage, volumes. |
| Production | Job failed after partial write? | Use Delta transactions, idempotent MERGE, checkpoints, retry-safe design. |
| Optimization | Small file fix? | Auto optimize/auto compact, OPTIMIZE, write repartitioning, avoid micro-batch tiny outputs. |

## 23.2 Scenario Questions

| Scenario | Best Response |
|---|---|
| Late files arrive after Gold built | Use incremental reprocessing by date or streaming watermark + update/merge Gold. |
| Need delete GDPR customer | Delta `DELETE`, propagate through Silver/Gold, run VACUUM after retention policy. |
| Schema changes unexpectedly | Auto Loader rescue column, schema evolution review, contract tests. |
| Duplicate source events | Deterministic window dedupe by key + event timestamp + ingestion timestamp. |
| BI query slow | OPTIMIZE/ZORDER, aggregate Gold table, partition pruning, Photon, cache serving table if appropriate. |
| JDBC read slow | Partitioned read with bounds, predicates, fetch size, avoid single connection bottleneck. |
| Streaming state grows | Add watermark, reduce key cardinality, tune trigger, ensure event-time column quality. |

---

# FINAL ULTRA-FAST REVISION CHEAT SHEET — Page 1 ⚡

## A. One-Liner Revision Notes

| Topic | One-Liner |
|---|---|
| Spark | Distributed compute engine; DataFrames are optimized by Catalyst. |
| Lazy Evaluation | Transformations wait until action, enabling whole-plan optimization. |
| Narrow vs Wide | Narrow avoids shuffle; wide causes shuffle. |
| Shuffle | Most expensive Spark operation; reduce, tune, or avoid. |
| Partitioning | Controls parallelism and pruning; do not over-partition by high-cardinality keys. |
| Broadcast Join | Best for small dimension + large fact join. |
| Cache | Use only for reused DataFrames; materialize and unpersist. |
| Delta | Adds ACID, MERGE, schema enforcement, time travel to data lake. |
| Auto Loader | Scalable incremental file ingestion with schema evolution. |
| COPY INTO | Simple idempotent batch file ingestion. |
| DLT | Declarative pipelines with expectations and lineage. |
| Unity Catalog | Governance layer for catalogs, schemas, tables, volumes, permissions. |

## B. PySpark Must-Remember Snippets

```python
# read/write Delta
spark.read.format('delta').table('main.sales.orders')
df.write.format('delta').mode('append').partitionBy('event_date').saveAsTable('main.sales.orders')

# filter/select
out = df.select('id', F.col('amount').cast('decimal(12,2)')).filter(F.col('amount') > 0)

# join/broadcast
out = fact.join(F.broadcast(dim), 'customer_id', 'left')

# aggregate
agg = df.groupBy('event_date').agg(F.count('*').alias('cnt'), F.sum('amount').alias('revenue'))

# latest record
w = Window.partitionBy('id').orderBy(F.col('updated_at').desc())
latest = df.withColumn('rn', F.row_number().over(w)).filter('rn=1').drop('rn')

# merge
DeltaTable.forName(spark,'main.sales.t').alias('t').merge(src.alias('s'),'t.id=s.id') \
  .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

🧾 **SQL Must-Remember**
```sql
MERGE INTO target t USING source s ON t.id=s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

OPTIMIZE main.sales.orders ZORDER BY (customer_id);
VACUUM main.sales.orders RETAIN 168 HOURS;
```

---

# FINAL ULTRA-FAST REVISION CHEAT SHEET — Page 2 ⚡

## C. Performance Shortcut Map

| If You See | Think |
|---|---|
| Slow join | Broadcast small side, check skew, check join keys/types. |
| One long task | Skew/hot key/spill. |
| Many tiny tasks | Too many partitions/files. |
| Slow scans | Need pruning, Delta/Parquet, OPTIMIZE/ZORDER. |
| Recomputed stages | Cache/persist reused intermediate. |
| Driver OOM | Avoid collect/toPandas; write distributed output. |
| Streaming duplicates | Check checkpoint, idempotent sink, source semantics. |
| State store huge | Watermark and reduce grouping cardinality. |

## D. Common Mistakes to Avoid

- ❌ `collect()` on large data.
- ❌ `coalesce(1)` for production output.
- ❌ `inferSchema` in stable production pipelines.
- ❌ Joining columns with mismatched types.
- ❌ Full-table overwrite when only one partition changed.
- ❌ Window without `partitionBy` for large data.
- ❌ Assuming `dropDuplicates` keeps latest row.
- ❌ Using Python UDF where built-ins exist.
- ❌ Forgetting streaming checkpoint location.
- ❌ Running `VACUUM` with unsafe retention for time travel/rollback needs.

## E. Final Interview Answers in 10 Seconds

| Prompt | Answer |
|---|---|
| Optimize Spark job | Read plan/UI, reduce scan, reduce shuffle, handle skew, compact files, broadcast, cache only reused data. |
| Design medallion | Bronze raw immutable, Silver validated/deduped/conformed, Gold aggregated business-ready. |
| Delta MERGE use | Upserts/CDC/SCD; match on business key, update matched, insert new, delete for CDC deletes. |
| Streaming reliability | Checkpoints, transactional Delta sink, watermarks, idempotent foreachBatch. |
| Unity Catalog benefit | Centralized permissions, lineage, auditing, external locations, volumes across workspaces. |

> **Last line to remember:** In interviews, always connect Spark code to production properties: correctness, scalability, observability, recoverability, and cost.
