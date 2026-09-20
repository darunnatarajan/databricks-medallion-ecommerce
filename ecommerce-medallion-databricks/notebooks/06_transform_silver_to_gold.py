# Databricks notebook source
# MAGIC %md
# MAGIC # Transform 03 : Silver to Gold
# MAGIC
# MAGIC **Medallion Architecture - Step 3 of 3**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `e_commerce.silver.*` |
# MAGIC | **Target** | `e_commerce.gold.*` |
# MAGIC | **Rule** | RESHAPE into a star schema and add a FEW new columns. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What we build
# MAGIC
# MAGIC **3 dimensions:** dim_date, dim_customer, dim_product
# MAGIC
# MAGIC **3 facts:** fact_sales, fact_reviews, fact_events
# MAGIC
# MAGIC **1 summary:** agg_sales_summary
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The new columns added in this layer
# MAGIC
# MAGIC | # | Table | New column | Logic |
# MAGIC |---|---|---|---|
# MAGIC | 1 | dim_customer | `signup_year` | year from signup_date |
# MAGIC | 2 | dim_product | `price_band` | Low / Medium / High from price |
# MAGIC | 3 | fact_sales | `is_completed` | Y if status is completed |
# MAGIC | 4 | fact_reviews | `sentiment` | Positive / Neutral / Negative from rating |
# MAGIC | 5 | fact_events | `is_purchase` | Y if event_type is purchase |
# MAGIC
# MAGIC Plus the surrogate keys `customer_key`, `product_key` and `date_key`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notebook template
# MAGIC
# MAGIC 1. Configuration
# MAGIC 2. Import libraries
# MAGIC 3. Helper function
# MAGIC 4. Build dimensions, then facts, then the summary
# MAGIC 5. Validation
# MAGIC 6. Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Configuration

# COMMAND ----------

# MAGIC %run ""

# COMMAND ----------

# ---------------------------------------------------------------
# Step 1 : Configuration  (now driven by widgets)
# ---------------------------------------------------------------
dbutils.widgets.removeAll()

dbutils.widgets.text("catalog",       "e_commerce", "1. Catalog")
dbutils.widgets.text("source_schema", "silver",     "2. Source Schema")
dbutils.widgets.text("target_schema", "gold",       "3. Target Schema")
dbutils.widgets.text("date_start",    "2023-01-01", "4. dim_date Start")
dbutils.widgets.text("date_end",      "2027-12-31", "5. dim_date End")

CATALOG        = dbutils.widgets.get("catalog")
SOURCE_CATALOG = CATALOG
TARGET_CATALOG = CATALOG
SOURCE_SCHEMA  = dbutils.widgets.get("source_schema")
TARGET_SCHEMA  = dbutils.widgets.get("target_schema")
DATE_START     = dbutils.widgets.get("date_start")
DATE_END       = dbutils.widgets.get("date_end")

print("Source    :", SOURCE_CATALOG + "." + SOURCE_SCHEMA)
print("Target    :", TARGET_CATALOG + "." + TARGET_SCHEMA)
print("Date range:", DATE_START, "to", DATE_END)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Import libraries

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Helper functions
# MAGIC
# MAGIC `read_silver` saves typing. `write_gold` writes the table and prints the count.
# MAGIC
# MAGIC **Important - the order we build things:**
# MAGIC
# MAGIC Dimensions first, then facts. The facts need the surrogate keys that the dimensions create, so a fact built first would get NULL keys.

# COMMAND ----------

def read_silver(table_name):
    """Read one table from the silver layer."""
    return spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + "." + table_name)


def write_gold(df, table_name):
    """Write a DataFrame to the gold layer and print the row count."""
    full_name = TARGET_CATALOG + "." + TARGET_SCHEMA + "." + table_name
    
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(full_name))
    
    print("Created", table_name, "->", spark.table(full_name).count(), "rows")
    return df


print("Helper functions ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4a : Build the dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension 1 : dim_date
# MAGIC
# MAGIC This table does not come from silver at all. We build it from a date range.
# MAGIC
# MAGIC **Why we need it:** if we counted days from the orders table, any day with zero orders would simply disappear from a report. A date dimension always has every day.

# COMMAND ----------

# SEQUENCE creates a list of dates, EXPLODE turns that list into rows
df_dates = spark.sql(
    "SELECT EXPLODE(SEQUENCE(DATE'" + DATE_START + "', DATE'" + DATE_END + "', INTERVAL 1 DAY)) AS full_date"
)

dim_date = df_dates.select(
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("full_date"),
    F.year("full_date").alias("year"),
    F.month("full_date").alias("month"),
    F.date_format("full_date", "MMMM").alias("month_name"),
    F.quarter("full_date").alias("quarter"),
    F.date_format("full_date", "EEEE").alias("day_name"),
    F.when(F.dayofweek("full_date").isin(1, 7), "Y").otherwise("N").alias("is_weekend"),
)

dim_date = write_gold(dim_date, "dim_date")
display(dim_date.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension 2 : dim_customer
# MAGIC
# MAGIC **New columns:** `customer_key` (surrogate key), `signup_year`
# MAGIC
# MAGIC **What is a surrogate key?** A simple number that replaces the text ID. `U000001` becomes `1`. Numbers join faster than text and keep the model tidy. We create it with `row_number()`, so you can see exactly where it comes from.

# COMMAND ----------

df_users = read_silver("users")

# Window tells row_number() what order to count in
window_users = Window.orderBy("user_id")

dim_customer = df_users.select(
    F.row_number().over(window_users).cast("bigint").alias("customer_key"),  # NEW
    F.col("user_id"),
    F.col("name"),
    F.col("email"),
    F.col("gender"),
    F.col("city"),
    F.col("signup_date"),
    F.year("signup_date").alias("signup_year"),  # NEW
)

dim_customer = write_gold(dim_customer, "dim_customer")
display(dim_customer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension 3 : dim_product
# MAGIC
# MAGIC **New columns:** `product_key` (surrogate key), `price_band`
# MAGIC
# MAGIC `price_band` groups products into Low, Medium and High so a report can compare cheap and expensive products without writing a CASE every time.

# COMMAND ----------

df_products = read_silver("products")

window_products = Window.orderBy("product_id")

dim_product = df_products.select(
    F.row_number().over(window_products).cast("bigint").alias("product_key"),  # NEW
    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("brand"),
    F.col("price"),
    F.col("rating"),
    F.when(F.col("price") < 100, "Low")
     .when(F.col("price") < 500, "Medium")
     .otherwise("High").alias("price_band"),  # NEW
)

dim_product = write_gold(dim_product, "dim_product")
display(dim_product.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4b : Build the facts
# MAGIC
# MAGIC Every fact follows the same 3 joins:
# MAGIC
# MAGIC 1. join to `dim_customer` to get `customer_key`
# MAGIC 2. join to `dim_product` to get `product_key`
# MAGIC 3. build `date_key` from the date column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact 1 : fact_sales
# MAGIC
# MAGIC Source: `silver.order_items` + `silver.orders` (we need the order date and status)
# MAGIC
# MAGIC **New columns:** `customer_key`, `product_key`, `date_key`, `is_completed`

# COMMAND ----------

df_items  = read_silver("order_items")
df_orders = read_silver("orders")

fact_sales = (
    df_items.alias("i")
    # bring in order_date and order_status from the order header
    .join(df_orders.alias("o"), 
          F.col("i.order_id") == F.col("o.order_id"), 
          "left")
    # get the surrogate keys from the dimensions
    .join(dim_customer.alias("c"), 
          F.col("i.user_id") == F.col("c.user_id"), 
          "left")
    .join(dim_product.alias("p"), 
          F.col("i.product_id") == F.col("p.product_id"), 
          "left")
    .select(
        F.col("i.order_item_id"),
        F.col("i.order_id"),
        F.col("c.customer_key"),  # NEW
        F.col("p.product_key"),  # NEW
        F.date_format("o.order_date", "yyyyMMdd").cast("int").alias("date_key"),  # NEW
        F.col("i.quantity"),
        F.col("i.item_price"),
        F.col("i.item_total"),
        F.col("o.order_status"),
        F.when(F.col("o.order_status") == "completed", "Y")
         .otherwise("N").alias("is_completed"),  # NEW
    )
)

fact_sales = write_gold(fact_sales, "fact_sales")
display(fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact 2 : fact_reviews
# MAGIC
# MAGIC **New columns:** `customer_key`, `product_key`, `date_key`, `sentiment`

# COMMAND ----------

df_reviews = read_silver("reviews")

fact_reviews = (
    df_reviews.alias("r")
    .join(dim_customer.alias("c"), 
          F.col("r.user_id") == F.col("c.user_id"), 
          "left")
    .join(dim_product.alias("p"), 
          F.col("r.product_id") == F.col("p.product_id"), 
          "left")
    .select(
        F.col("r.review_id"),
        F.col("r.order_id"),
        F.col("c.customer_key"),  # NEW
        F.col("p.product_key"),  # NEW
        F.date_format("r.review_date", "yyyyMMdd").cast("int").alias("date_key"),  # NEW
        F.col("r.rating"),
        F.col("r.review_text"),
        F.when(F.col("r.rating") <= 2, "Negative")
         .when(F.col("r.rating") == 3, "Neutral")
         .otherwise("Positive").alias("sentiment"),  # NEW
    )
)

fact_reviews = write_gold(fact_reviews, "fact_reviews")
display(fact_reviews.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact 3 : fact_events
# MAGIC
# MAGIC **New columns:** `customer_key`, `product_key`, `date_key`, `is_purchase`

# COMMAND ----------

df_events = read_silver("events")

fact_events = (
    df_events.alias("e")
    .join(dim_customer.alias("c"), 
          F.col("e.user_id") == F.col("c.user_id"), 
          "left")
    .join(dim_product.alias("p"), 
          F.col("e.product_id") == F.col("p.product_id"), 
          "left")
    .select(
        F.col("e.event_id"),
        F.col("c.customer_key"),  # NEW
        F.col("p.product_key"),  # NEW
        F.date_format("e.event_timestamp", "yyyyMMdd").cast("int").alias("date_key"),  # NEW
        F.col("e.event_type"),
        F.col("e.event_timestamp"),
        F.when(F.col("e.event_type") == "purchase", "Y")
         .otherwise("N").alias("is_purchase"),  # NEW
    )
)

fact_events = write_gold(fact_events, "fact_events")
display(fact_events.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4c : Build the summary table
# MAGIC
# MAGIC A small pre-calculated table. Instead of a report scanning 43,525 sales rows every time, it reads a few hundred summary rows.
# MAGIC
# MAGIC Revenue counts only completed orders, so cancelled orders do not inflate it.

# COMMAND ----------

agg_sales_summary = (
    fact_sales.alias("s")
    .join(dim_product.alias("p"), 
          F.col("s.product_key") == F.col("p.product_key"), 
          "left")
    .groupBy("s.date_key", "p.category")
    .agg(
        F.countDistinct("s.order_id").alias("total_orders"),
        F.sum("s.quantity").cast("bigint").alias("total_quantity"),
        F.sum(
            F.when(F.col("s.is_completed") == "Y", F.col("s.item_total"))
             .otherwise(0)
        ).cast("decimal(14,2)").alias("total_revenue"),
    )
    .select("date_key", "category", "total_orders", "total_quantity", "total_revenue")
)

agg_sales_summary = write_gold(agg_sales_summary, "agg_sales_summary")
display(agg_sales_summary.orderBy(F.desc("total_revenue")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 : Validation

# COMMAND ----------

tables = [
    "dim_date", "dim_customer", "dim_product",
    "fact_sales", "fact_reviews", "fact_events", "agg_sales_summary"
]

print("GOLD LAYER ROW COUNTS")
print("-" * 36)

for t in tables:
    count = spark.table(TARGET_CATALOG + "." + TARGET_SCHEMA + "." + t).count()
    print(t.ljust(20), count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for missing keys
# MAGIC
# MAGIC A NULL key means a join did not find a match. All of these should be 0.

# COMMAND ----------

print("Missing keys in fact_sales")
print("  customer_key :", fact_sales.filter(F.col("customer_key").isNull()).count())
print("  product_key  :", fact_sales.filter(F.col("product_key").isNull()).count())
print("  date_key     :", fact_sales.filter(F.col("date_key").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample business question: top 5 categories by revenue

# COMMAND ----------

# Query the summary table to answer business questions quickly
display(spark.sql("""
    SELECT category,
           SUM(total_revenue) AS revenue,
           SUM(total_quantity) AS units
    FROM   e_commerce.gold.agg_sales_summary
    GROUP  BY category
    ORDER  BY revenue DESC
    LIMIT  5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done!
# MAGIC
# MAGIC The full medallion pipeline is complete.
# MAGIC
# MAGIC | Layer | What it holds |
# MAGIC |---|---|
# MAGIC | Bronze | Raw data, exactly as the CSV had it |
# MAGIC | Silver | Same columns, cleaned values |
# MAGIC | Gold | Star schema with 5 new business columns |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC Connect Power BI to `e_commerce.gold` and join the facts to the dimensions using the `_key` columns.