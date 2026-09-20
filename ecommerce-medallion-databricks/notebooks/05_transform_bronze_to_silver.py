# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # Transform 02: Bronze to Silver
# MAGIC
# MAGIC **Medallion Architecture - Step 2 of 3**
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `e_commerce.bronze.*` |
# MAGIC | **Target** | `e_commerce.silver.*` |
# MAGIC | **Rule** | CLEAN ONLY. Same tables, same columns, same data types. No new columns. |
# MAGIC
# MAGIC ### The 5 cleaning rules applied to every table
# MAGIC
# MAGIC | # | Rule | Example |
# MAGIC |---|---|---|
# MAGIC | 1 | Trim extra spaces | `"  Chennai  "` → `"Chennai"` |
# MAGIC | 2 | Empty text becomes NULL | `""` → `NULL` |
# MAGIC | 3 | Fix the case | `"JOHN@MAIL.COM"` → `"john@mail.com"` |
# MAGIC | 4 | Remove duplicate IDs | keep 1 row per ID |
# MAGIC | 5 | Remove rows with no ID | drop where ID is NULL |
# MAGIC
# MAGIC ### Notebook template
# MAGIC
# MAGIC 1. Configuration
# MAGIC 2. Import libraries
# MAGIC 3. Helper functions
# MAGIC 4. Clean each table (Read → Clean → Write → Check)
# MAGIC 5. Validation
# MAGIC 6. Summary

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

# DBTITLE 1,Configuration variables
# ---------------------------------------------------------------
# Step 1 : Configuration  (now driven by widgets)
# ---------------------------------------------------------------
dbutils.widgets.removeAll()

dbutils.widgets.text("catalog",       "e_commerce", "1. Catalog")
dbutils.widgets.text("source_schema", "bronze",     "2. Source Schema")
dbutils.widgets.text("target_schema", "silver",     "3. Target Schema")
dbutils.widgets.dropdown("load_mode", "overwrite", ["overwrite", "append"], "4. Load Mode")

CATALOG        = dbutils.widgets.get("catalog")
SOURCE_CATALOG = CATALOG
TARGET_CATALOG = CATALOG
SOURCE_SCHEMA  = dbutils.widgets.get("source_schema")
TARGET_SCHEMA  = dbutils.widgets.get("target_schema")
LOAD_MODE      = dbutils.widgets.get("load_mode")

print("Source :", SOURCE_CATALOG + "." + SOURCE_SCHEMA)
print("Target :", TARGET_CATALOG + "." + TARGET_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC ## Step 2: Import Libraries

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql import functions as F

print("Libraries imported")

# COMMAND ----------

# DBTITLE 1,Step 3 Header
# MAGIC %md
# MAGIC ## Step 3: Helper Functions
# MAGIC
# MAGIC Four small functions. Each one does a single job, so the cleaning code for each table stays short and easy to read.

# COMMAND ----------

# DBTITLE 1,Define helper functions
def clean_text(column_name):
    """Rule 1 + 2 : trim spaces, and turn empty text into NULL."""
    trimmed = F.trim(F.col(column_name))
    return F.when(trimmed == "", None).otherwise(trimmed).alias(column_name)

def clean_lower(column_name):
    """Rule 3 : trim and convert to lowercase. Used for email, status, event_type."""
    trimmed = F.lower(F.trim(F.col(column_name)))
    return F.when(trimmed == "", None).otherwise(trimmed).alias(column_name)

def clean_proper(column_name):
    """Rule 3 : trim and capitalise the first letter. Used for gender, city, category."""
    trimmed = F.initcap(F.trim(F.col(column_name)))
    return F.when(trimmed == "", None).otherwise(trimmed).alias(column_name)

def write_silver(df, table_name, id_column):
    """Rule 4 + 5 : remove duplicates and NULL IDs, then write to silver."""
    before = df.count()
    df_clean = (df
                .filter(F.col(id_column).isNotNull())   # Rule 5
                .dropDuplicates([id_column]))           # Rule 4
    after = df_clean.count()
    full_name = TARGET_CATALOG + "." + TARGET_SCHEMA + "." + table_name
    (df_clean.write.format("delta")
             .mode(LOAD_MODE)
             .saveAsTable(full_name))
    print("Cleaned", table_name, ":", before, "rows in ->", after, "rows out",
          "( removed", before - after, ")")
    return df_clean

print("Helper functions ready")

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC ## Step 4: Clean Each Table

# COMMAND ----------

# DBTITLE 1,Table 1 Header
# MAGIC %md
# MAGIC ### Table 1: users
# MAGIC
# MAGIC * `name`, `city` → trim + capitalize
# MAGIC * `email` → trim + lowercase
# MAGIC * `gender` → trim + capitalize

# COMMAND ----------

# DBTITLE 1,Clean users table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".users")

df_users = df.select(
    clean_text("user_id"),
    clean_text("name"),
    clean_lower("email"),
    clean_proper("gender"),
    clean_proper("city"),
    F.col("signup_date"),
)

df_users = write_silver(df_users, "users", "user_id")
display(df_users.limit(5))

# COMMAND ----------

# DBTITLE 1,Table 2 Header
# MAGIC %md
# MAGIC ### Table 2: products
# MAGIC
# MAGIC * `category` → trim + capitalize
# MAGIC * `price`, `rating` → no change, already numbers

# COMMAND ----------

# DBTITLE 1,Clean products table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".products")

df_products = df.select(
    clean_text("product_id"),
    clean_text("product_name"),
    clean_proper("category"),
    clean_text("brand"),
    F.col("price"),
    F.col("rating"),
)

df_products = write_silver(df_products, "products", "product_id")
display(df_products.limit(5))

# COMMAND ----------

# DBTITLE 1,Table 3 Header
# MAGIC %md
# MAGIC ### Table 3: orders
# MAGIC
# MAGIC * `order_status` → trim + lowercase, so `"Completed"` and `"completed"` become one value

# COMMAND ----------

# DBTITLE 1,Clean orders table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".orders")

df_orders = df.select(
    clean_text("order_id"),
    clean_text("user_id"),
    F.col("order_date"),
    clean_lower("order_status"),
    F.col("total_amount"),
)

df_orders = write_silver(df_orders, "orders", "order_id")
display(df_orders.limit(5))

# COMMAND ----------

# DBTITLE 1,Table 4 Header
# MAGIC %md
# MAGIC ### Table 4: order_items
# MAGIC
# MAGIC * IDs → trim only
# MAGIC * numbers → no change

# COMMAND ----------

# DBTITLE 1,Clean order_items table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".order_items")

df_order_items = df.select(
    clean_text("order_item_id"),
    clean_text("order_id"),
    clean_text("product_id"),
    clean_text("user_id"),
    F.col("quantity"),
    F.col("item_price"),
    F.col("item_total"),
)

df_order_items = write_silver(df_order_items, "order_items", "order_item_id")
display(df_order_items.limit(5))

# COMMAND ----------

# DBTITLE 1,Table 5 Header
# MAGIC %md
# MAGIC ### Table 5: reviews
# MAGIC
# MAGIC * `review_text` → trim only, we do not change the customer's words

# COMMAND ----------

# DBTITLE 1,Clean reviews table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".reviews")

df_reviews = df.select(
    clean_text("review_id"),
    clean_text("order_id"),
    clean_text("product_id"),
    clean_text("user_id"),
    F.col("rating"),
    clean_text("review_text"),
    F.col("review_date"),
)

df_reviews = write_silver(df_reviews, "reviews", "review_id")
display(df_reviews.limit(5))

# COMMAND ----------

# DBTITLE 1,Table 6 Header
# MAGIC %md
# MAGIC ### Table 6: events
# MAGIC
# MAGIC * `event_type` → trim + lowercase

# COMMAND ----------

# DBTITLE 1,Clean events table
df = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + ".events")

df_events = df.select(
    clean_text("event_id"),
    clean_text("user_id"),
    clean_text("product_id"),
    clean_lower("event_type"),
    F.col("event_timestamp"),
)

df_events = write_silver(df_events, "events", "event_id")
display(df_events.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 5 Header
# MAGIC %md
# MAGIC ## Step 5: Validation
# MAGIC
# MAGIC Compare bronze and silver row counts. A difference means duplicates or NULL IDs were removed - that is the cleaning working, not an error.

# COMMAND ----------

# DBTITLE 1,Validate row counts
tables = ["users", "products", "orders", "order_items", "reviews", "events"]

print("TABLE".ljust(15), "BRONZE".rjust(8), "SILVER".rjust(8), "REMOVED".rjust(8))
print("-" * 45)

for t in tables:
    b = spark.table(SOURCE_CATALOG + "." + SOURCE_SCHEMA + "." + t).count()
    s = spark.table(TARGET_CATALOG + "." + TARGET_SCHEMA + "." + t).count()
    print(t.ljust(15), str(b).rjust(8), str(s).rjust(8), str(b - s).rjust(8))

# COMMAND ----------

# DBTITLE 1,Verification Header
# MAGIC %md
# MAGIC ### Verify the cleaning worked

# COMMAND ----------

# DBTITLE 1,Verify cleaning worked
# The status values should now all be lowercase
print("Order status values:")
display(spark.table(TARGET_CATALOG + "." + TARGET_SCHEMA + ".orders")
             .groupBy("order_status").count().orderBy("order_status"))

# COMMAND ----------

# DBTITLE 1,Step 6 Header
# MAGIC %md
# MAGIC ## Step 6: Summary

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ### ✅ Done!
# MAGIC
# MAGIC All 6 tables are cleaned and loaded into **silver**.
# MAGIC
# MAGIC Notice that silver has exactly the same columns as bronze. We only changed the **values**, never the **structure**. New columns come next.
# MAGIC
# MAGIC **Next:** `06_transform_silver_to_gold`