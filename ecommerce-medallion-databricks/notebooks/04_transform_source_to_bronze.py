# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Transform 01: Source to Bronze
# MAGIC
# MAGIC **Medallion Architecture - Step 1 of 3**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | CSV files in `/Volumes/e_commerce/source/dataload/` |
# MAGIC | **Target** | `e_commerce.bronze.*` |
# MAGIC | **Rule** | Load the data AS IT IS. No cleaning in this layer. |
# MAGIC
# MAGIC ### Notebook Template (same in all 3 notebooks)
# MAGIC
# MAGIC 1. Configuration
# MAGIC 2. Import libraries
# MAGIC 3. Helper function
# MAGIC 4. Load each table (Read → Write → Check)
# MAGIC 5. Validation
# MAGIC 6. Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Configuration

# COMMAND ----------

# DBTITLE 1,Cell 3
# ---------------------------------------------------------------
# Step 1 : Configuration  (now driven by widgets)
# ---------------------------------------------------------------

# removeAll clears old widgets so a re-run always starts clean
dbutils.widgets.removeAll()

# --- create the widgets ---
dbutils.widgets.text("catalog",      "e_commerce",                         "1. Catalog")
dbutils.widgets.text("schema",       "bronze",                             "2. Schema")
dbutils.widgets.text("volume_path",  "/Volumes/e_commerce/source/dataload", "3. Volume Path")
dbutils.widgets.dropdown("load_mode", "overwrite", ["overwrite", "append"], "4. Load Mode")

# --- read the values into variables ---
CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
VOLUME_PATH = dbutils.widgets.get("volume_path").rstrip("/")
LOAD_MODE   = dbutils.widgets.get("load_mode")

print("Source    :", VOLUME_PATH)
print("Target    :", CATALOG + "." + SCHEMA)
print("Load mode :", LOAD_MODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Import libraries

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, TimestampType, DecimalType
)

print("✓ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC ## Step 3: Helper Function
# MAGIC
# MAGIC One function used by every table below. Written once, used six times.
# MAGIC
# MAGIC **Why we give a schema instead of using `inferSchema`:**
# MAGIC
# MAGIC * `inferSchema` makes Spark read the file twice and guess the data types.
# MAGIC * Giving the schema is faster and the types are always the same.

# COMMAND ----------

# DBTITLE 1,Helper Function
def load_to_bronze(file_name, schema, table_name):
    """Read one CSV file and write it to a bronze Delta table."""
    
    # READ
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("multiLine", "true")
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")
          .schema(schema)
          .load(VOLUME_PATH + "/" + file_name))
    
    # WRITE
    full_name = CATALOG + "." + SCHEMA + "." + table_name
    (df.write.format("delta")
       .mode(LOAD_MODE)                # was: .mode("overwrite")
       .saveAsTable(full_name))
    
    # CHECK
    row_count = spark.table(full_name).count()
    print(f"✓ Loaded {table_name} → {row_count:,} rows")
    return df

print("✓ Helper function ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 : Load each table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1 : users

# COMMAND ----------

# DBTITLE 1,Cell 10
users_schema = StructType([
    StructField("user_id",     StringType(), True),
    StructField("name",        StringType(), True),
    StructField("email",       StringType(), True),
    StructField("gender",      StringType(), True),
    StructField("city",        StringType(), True),
    StructField("signup_date", DateType(),   True),
])

df_users = load_to_bronze("users.csv", users_schema, "users")
display(df_users.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2 : products

# COMMAND ----------

# DBTITLE 1,Cell 12
products_schema = StructType([
    StructField("product_id",   StringType(),       True),
    StructField("product_name", StringType(),       True),
    StructField("category",     StringType(),       True),
    StructField("brand",        StringType(),       True),
    StructField("price",        DecimalType(10, 2), True),
    StructField("rating",       DecimalType(3, 2),  True),
])

df_products = load_to_bronze("products.csv", products_schema, "products")
display(df_products.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 3 : orders

# COMMAND ----------

# DBTITLE 1,Cell 14
orders_schema = StructType([
    StructField("order_id",     StringType(),       True),
    StructField("user_id",      StringType(),       True),
    StructField("order_date",   TimestampType(),    True),
    StructField("order_status", StringType(),       True),
    StructField("total_amount", DecimalType(12, 2), True),
])

df_orders = load_to_bronze("orders.csv", orders_schema, "orders")
display(df_orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 4 : order_items

# COMMAND ----------

# DBTITLE 1,Cell 16
order_items_schema = StructType([
    StructField("order_item_id", StringType(),       True),
    StructField("order_id",      StringType(),       True),
    StructField("product_id",    StringType(),       True),
    StructField("user_id",       StringType(),       True),
    StructField("quantity",      IntegerType(),      True),
    StructField("item_price",    DecimalType(10, 2), True),
    StructField("item_total",    DecimalType(12, 2), True),
])

df_order_items = load_to_bronze("order_items.csv", order_items_schema, "order_items")
display(df_order_items.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 5 : reviews

# COMMAND ----------

# DBTITLE 1,Cell 18
reviews_schema = StructType([
    StructField("review_id",   StringType(),    True),
    StructField("order_id",    StringType(),    True),
    StructField("product_id",  StringType(),    True),
    StructField("user_id",     StringType(),    True),
    StructField("rating",      IntegerType(),   True),
    StructField("review_text", StringType(),    True),
    StructField("review_date", TimestampType(), True),
])

df_reviews = load_to_bronze("reviews.csv", reviews_schema, "reviews")
display(df_reviews.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 6 : events

# COMMAND ----------

# DBTITLE 1,Cell 20
events_schema = StructType([
    StructField("event_id",        StringType(),    True),
    StructField("user_id",         StringType(),    True),
    StructField("product_id",      StringType(),    True),
    StructField("event_type",      StringType(),    True),
    StructField("event_timestamp", TimestampType(), True),
])

df_events = load_to_bronze("events.csv", events_schema, "events")
display(df_events.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 21
# MAGIC %md
# MAGIC ## Step 5: Validation
# MAGIC
# MAGIC **Expected row counts:** 10,000 / 2,000 / 20,000 / 43,525 / 15,000 / 80,000

# COMMAND ----------

# DBTITLE 1,Cell 22
tables = ["users", "products", "orders", "order_items", "reviews", "events"]

print("\n" + "=" * 40)
print("BRONZE LAYER ROW COUNTS")
print("=" * 40)

for t in tables:
    count = spark.table(CATALOG + "." + SCHEMA + "." + t).count()
    print(f"{t.ljust(15)} {count:,}")

print("=" * 40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 : Summary

# COMMAND ----------

# DBTITLE 1,Cell 24
# MAGIC %md
# MAGIC ✅ **Done.** All 6 CSV files are now bronze Delta tables.
# MAGIC
# MAGIC The data is still raw - extra spaces, mixed upper/lower case and any duplicates are all still there. That is correct. Cleaning happens in the next notebook.
# MAGIC
# MAGIC **Next:** `05_transform_bronze_to_silver`