# Practical 8: Distributed Data Processing and Analytics with Apache PySpark

**Author:** Wali Momin

---

## 1. Aim
To utilize Apache PySpark for big data analytics: initializing a SparkSession, inferring schemas, performing distributed transformations, filtering, aggregations, deduplication, and multi-dataset joins.

---

## 2. Brief Theory
Apache Spark is a unified analytics engine for large-scale distributed data processing:
- **Spark Architecture:** Master-Worker topology utilizing in-memory computation, outperforming traditional disk-based MapReduce systems.
- **SparkSession:** Unified entry point to programming Spark with DataFrame and Dataset APIs.
- **Spark DataFrames:** Distributed collections of data organized into named columns with schema enforcement.
- **Core Operations:**
  - **Transformations (Lazy Evaluation):** `filter()`, `withColumn()`, `dropDuplicates()`, `groupBy()`, `join()`. Execution is deferred until an action is invoked.
  - **Actions (Eager Execution):** `count()`, `show()`, `collect()`. Triggers computation across the DAG.
  - **Spark SQL Functions:** Built-in optimized functions (`F.col`, `F.sum`, `F.avg`, `F.round`).

---

## 3. Code
```python
!pip install -q pyspark

import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STUDENT_NAME = "Wali Momin"
CSV_PATH = os.path.abspath("sales.csv")

# Start a local Spark session
spark = (SparkSession.builder
         .appName("Practical8_PySpark")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def print_header(title):
    print("\n" + "=" * 70 + "\n" + title + "\n" + "=" * 70)

def create_sales_csv():
    """Creates the sample sales dataset (with two duplicate rows)."""
    rows = [
        (1, "Laptop",   "Electronics", 55000, 2),
        (2, "Phone",    "Electronics", 20000, 5),
        (2, "Phone",    "Electronics", 20000, 5),    # duplicate
        (3, "Tablet",   "Electronics", 30000, 3),
        (4, "Shirt",    "Clothing",      800, 10),
        (5, "Jeans",    "Clothing",     1500, 4),
        (5, "Jeans",    "Clothing",     1500, 4),    # duplicate
        (6, "Sofa",     "Furniture",   25000, 1),
        (7, "Chair",    "Furniture",    3500, 6),
        (8, "Notebook", "Stationery",     60, 50),
    ]
    with open("sales.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "product", "category", "price", "quantity"])
        writer.writerows(rows)
    print("Created sales.csv with", len(rows), "rows")


create_sales_csv()

print_header("TASK 1: Read a CSV dataset and display its schema")
sales_df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

print("Schema:")
sales_df.printSchema()

print("Data (", sales_df.count(), "rows ):")
sales_df.show()

print_header("TASK 2: Filtering, grouping and aggregation")
# Add a 'sales' column = price x quantity
sales_df = sales_df.withColumn("sales", F.col("price") * F.col("quantity"))

print("Filter 1: price > 1000")
sales_df.filter(F.col("price") > 1000).show()

print("Filter 2: category = 'Electronics'")
sales_df.filter(F.col("category") == "Electronics").show()

print("Filter 3: price > 1000 AND quantity >= 4")
sales_df.filter((F.col("price") > 1000) & (F.col("quantity") >= 4)).show()

print("Grouping and aggregation by category (before removing duplicates):")
(sales_df.groupBy("category")
    .agg(F.count("*").alias("orders"),
         F.sum("sales").alias("total_sales"),
         F.max("price").alias("max_price"),
         F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.desc("total_sales"))
    .show())

print_header("TASK 3: Remove duplicate records")
rows_before = sales_df.count()
dedup_df = sales_df.dropDuplicates()
rows_after = dedup_df.count()
print("Rows before :", rows_before)
print("Rows after  :", rows_after)
print("Duplicates removed:", rows_before - rows_after)

print("Duplicate order_id values in the original data:")
sales_df.groupBy("order_id").count().filter(F.col("count") > 1).show()

print("Dataset after removing duplicates:")
dedup_df.orderBy("order_id").show()

print_header("TASK 4: Join two datasets")
discounts_df = spark.createDataFrame(
    [("Electronics", 10), ("Clothing", 20), ("Furniture", 5)],
    ["category", "discount_pct"])
print("Second dataset (discounts by category):")
discounts_df.show()

print("Inner join (only categories present in both datasets):")
dedup_df.join(discounts_df, on="category", how="inner").orderBy("order_id").show()

print("Left join (all products kept; no discount -> null):")
joined_df = dedup_df.join(discounts_df, on="category", how="left")
joined_df.orderBy("order_id").show()

print("Sales after discount:")
(joined_df
    .withColumn("discounted_sales",
                F.round(F.col("sales") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0)) / 100), 2))
    .select("order_id", "product", "category", "sales", "discount_pct", "discounted_sales")
    .orderBy("order_id")
    .show())

print_header("TASK 5: Average sales by product category")

print("Using DataFrame API:")
avg_df = (dedup_df.groupBy("category")
          .agg(F.round(F.avg("sales"), 2).alias("avg_sales"))
          .orderBy(F.desc("avg_sales")))
avg_df.show()

print("Using Spark SQL:")
dedup_df.createOrReplaceTempView("sales")
spark.sql("""
    SELECT category, ROUND(AVG(sales), 2) AS avg_sales
    FROM sales
    GROUP BY category
    ORDER BY avg_sales DESC
""").show()

print("\n" + "=" * 40)
print("Submitted by:", STUDENT_NAME)
print("=" * 40)

spark.stop()
```

---

## 4. Output
```text
Created sales.csv with 10 rows

======================================================================
TASK 1: Read a CSV dataset and display its schema
======================================================================
Schema:
root
 |-- order_id: integer (nullable = true)
 |-- product: string (nullable = true)
 |-- category: string (nullable = true)
 |-- price: integer (nullable = true)
 |-- quantity: integer (nullable = true)

Data ( 10 rows ):
+--------+--------+-----------+-----+--------+
|order_id| product|   category|price|quantity|
+--------+--------+-----------+-----+--------+
|       1|  Laptop|Electronics|55000|       2|
|       2|   Phone|Electronics|20000|       5|
|       2|   Phone|Electronics|20000|       5|
|       3|  Tablet|Electronics|30000|       3|
|       4|   Shirt|   Clothing|  800|      10|
|       5|   Jeans|   Clothing| 1500|       4|
|       5|   Jeans|   Clothing| 1500|       4|
|       6|    Sofa|  Furniture|25000|       1|
|       7|   Chair|  Furniture| 3500|       6|
|       8|Notebook| Stationery|   60|      50|
+--------+--------+-----------+-----+--------+

======================================================================
TASK 2: Filtering, grouping and aggregation
======================================================================
Filter 1: price > 1000
+--------+-------+-----------+-----+--------+------+
|order_id|product|   category|price|quantity| sales|
+--------+-------+-----------+-----+--------+------+
|       1| Laptop|Electronics|55000|       2|110000|
|       2|  Phone|Electronics|20000|       5|100000|
|       2|  Phone|Electronics|20000|       5|100000|
|       3| Tablet|Electronics|30000|       3| 90000|
|       5|  Jeans|   Clothing| 1500|       4|  6000|
|       5|  Jeans|   Clothing| 1500|       4|  6000|
|       6|   Sofa|  Furniture|25000|       1| 25000|
|       7|  Chair|  Furniture| 3500|       6| 21000|
+--------+-------+-----------+-----+--------+------+

Filter 2: category = 'Electronics'
+--------+-------+-----------+-----+--------+------+
|order_id|product|   category|price|quantity| sales|
+--------+-------+-----------+-----+--------+------+
|       1| Laptop|Electronics|55000|       2|110000|
|       2|  Phone|Electronics|20000|       5|100000|
|       2|  Phone|Electronics|20000|       5|100000|
|       3| Tablet|Electronics|30000|       3| 90000|
+--------+-------+-----------+-----+--------+------+

Filter 3: price > 1000 AND quantity >= 4
+--------+-------+-----------+-----+--------+------+
|order_id|product|   category|price|quantity| sales|
+--------+-------+-----------+-----+--------+------+
|       2|  Phone|Electronics|20000|       5|100000|
|       2|  Phone|Electronics|20000|       5|100000|
|       5|  Jeans|   Clothing| 1500|       4|  6000|
|       5|  Jeans|   Clothing| 1500|       4|  6000|
|       7|  Chair|  Furniture| 3500|       6| 21000|
+--------+-------+-----------+-----+--------+------+

Grouping and aggregation by category (before removing duplicates):
+-----------+------+-----------+---------+---------+
|   category|orders|total_sales|max_price|avg_price|
+-----------+------+-----------+---------+---------+
|Electronics|     4|     400000|    55000|  31250.0|
|  Furniture|     2|      46000|    25000|  14250.0|
|   Clothing|     3|      20000|     1500|  1266.67|
| Stationery|     1|       3000|       60|     60.0|
+-----------+------+-----------+---------+---------+

======================================================================
TASK 3: Remove duplicate records
======================================================================
Rows before : 10
Rows after  : 8
Duplicates removed: 2
Duplicate order_id values in the original data:
+--------+-----+
|order_id|count|
+--------+-----+
|       5|    2|
|       2|    2|
+--------+-----+

Dataset after removing duplicates:
+--------+--------+-----------+-----+--------+------+
|order_id| product|   category|price|quantity| sales|
+--------+--------+-----------+-----+--------+------+
|       1|  Laptop|Electronics|55000|       2|110000|
|       2|   Phone|Electronics|20000|       5|100000|
|       3|  Tablet|Electronics|30000|       3| 90000|
|       4|   Shirt|   Clothing|  800|      10|  8000|
|       5|   Jeans|   Clothing| 1500|       4|  6000|
|       6|    Sofa|  Furniture|25000|       1| 25000|
|       7|   Chair|  Furniture| 3500|       6| 21000|
|       8|Notebook| Stationery|   60|      50|  3000|
+--------+--------+-----------+-----+--------+------+

======================================================================
TASK 4: Join two datasets
======================================================================
Second dataset (discounts by category):
+-----------+------------+
|   category|discount_pct|
+-----------+------------+
|Electronics|          10|
|   Clothing|          20|
|  Furniture|           5|
+-----------+------------+

Inner join (only categories present in both datasets):
+-----------+--------+-------+-----+--------+------+------------+
|   category|order_id|product|price|quantity| sales|discount_pct|
+-----------+--------+-------+-----+--------+------+------------+
|Electronics|       1| Laptop|55000|       2|110000|          10|
|Electronics|       2|  Phone|20000|       5|100000|          10|
|Electronics|       3| Tablet|30000|       3| 90000|          10|
|   Clothing|       4|  Shirt|  800|      10|  8000|          20|
|   Clothing|       5|  Jeans| 1500|       4|  6000|          20|
|  Furniture|       6|   Sofa|25000|       1| 25000|           5|
|  Furniture|       7|  Chair| 3500|       6| 21000|           5|
+-----------+--------+-------+-----+--------+------+------------+

Left join (all products kept; no discount -> null):
+-----------+--------+--------+-----+--------+------+------------+
|   category|order_id| product|price|quantity| sales|discount_pct|
+-----------+--------+--------+-----+--------+------+------------+
|Electronics|       1|  Laptop|55000|       2|110000|          10|
|Electronics|       2|   Phone|20000|       5|100000|          10|
|Electronics|       3|  Tablet|30000|       3| 90000|          10|
|   Clothing|       4|   Shirt|  800|      10|  8000|          20|
|   Clothing|       5|   Jeans| 1500|       4|  6000|          20|
|  Furniture|       6|    Sofa|25000|       1| 25000|           5|
|  Furniture|       7|   Chair| 3500|       6| 21000|           5|
| Stationery|       8|Notebook|   60|      50|  3000|        NULL|
+-----------+--------+--------+-----+--------+------+------------+

Sales after discount:
+--------+--------+-----------+------+------------+----------------+
|order_id| product|   category| sales|discount_pct|discounted_sales|
+--------+--------+-----------+------+------------+----------------+
|       1|  Laptop|Electronics|110000|          10|         99000.0|
|       2|   Phone|Electronics|100000|          10|         90000.0|
|       3|  Tablet|Electronics| 90000|          10|         81000.0|
|       4|   Shirt|   Clothing|  8000|          20|          6400.0|
|       5|   Jeans|   Clothing|  6000|          20|          4800.0|
|       6|    Sofa|  Furniture| 25000|           5|         23750.0|
|       7|   Chair|  Furniture| 21000|           5|         19950.0|
|       8|Notebook| Stationery|  3000|        NULL|          3000.0|
+--------+--------+-----------+------+------------+----------------+

======================================================================
TASK 5: Average sales by product category
======================================================================
Using DataFrame API:
+-----------+---------+
|   category|avg_sales|
+-----------+---------+
|Electronics| 100000.0|
|  Furniture|  23000.0|
|   Clothing|   7000.0|
| Stationery|   3000.0|
+-----------+---------+

Using Spark SQL:
+-----------+---------+
|   category|avg_sales|
+-----------+---------+
|Electronics| 100000.0|
|  Furniture|  23000.0|
|   Clothing|   7000.0|
| Stationery|   3000.0|
+-----------+---------+

========================================
Submitted by: Wali Momin
========================================
```

---

## 5. Conclusion
Successfully executed distributed data transformations in PySpark. Demonstrated schema inference, duplicate removal, calculated column derivation, relational joining, and grouped aggregations using the DataFrame API.
