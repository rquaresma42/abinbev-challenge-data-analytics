# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer - Data Cleaning and Validation
# MAGIC 
# MAGIC **Purpose**: Clean, validate, and normalize data from Bronze layer.
# MAGIC 
# MAGIC **Transformations**:
# MAGIC - Data type conversions
# MAGIC - Duplicate removal
# MAGIC - Integrity validations (revenue > 0, valid dates)
# MAGIC - **Merge users + targets** into single dimension
# MAGIC - Normalize text fields (lowercase, trim)
# MAGIC 
# MAGIC **Input**: Bronze tables
# MAGIC **Output**: Silver tables ready for dimensional modeling

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, to_date, year, month

print("🔧 SILVER LAYER - Cleaning and Validation")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders - Fact Table Preparation

# COMMAND ----------

# === SILVER ORDERS ===
df_silver_orders = (
    spark.table("abinbev.default.bronze_raw_orders")
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("user_id", col("user_id").cast("long"))
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("revenue", col("revenue").cast("double"))
    .dropDuplicates(["order_id", "product_id"])
    .filter(col("revenue") > 0)
    .filter(col("order_date").isNotNull())
    .filter(year(col("order_date")) == 2024)
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
)

print(f"✅ silver_orders → {df_silver_orders.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users - Merge with Targets
# MAGIC 
# MAGIC Combine user information with their monthly revenue targets into a single dimension.

# COMMAND ----------

# === SILVER USERS (MERGED WITH TARGETS) ===
df_users = spark.table("abinbev.default.bronze_raw_users")
df_targets = spark.table("abinbev.default.bronze_raw_targets")

df_silver_users = (
    df_users
    .join(
        df_targets.select("user_id", col("monthly_revenue_target")),
        on="user_id",
        how="left"
    )
    .select(
        col("user_id").cast("long"),
        trim(lower(col("category"))).alias("category"),
        trim(col("city")).alias("city"),
        col("monthly_revenue_target").cast("double").alias("monthly_revenue_target")
    )
    .dropDuplicates(["user_id"])
)

print(f"✅ silver_users → {df_silver_users.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Items - Product Dimension

# COMMAND ----------

# === SILVER ITEMS ===
df_silver_items = (
    spark.table("abinbev.default.bronze_raw_items")
    .withColumn("item_id", col("item_id").cast("long"))
    .withColumn("category", trim(lower(col("category"))))
    .filter(col("category").isin(["beer", "nab", "soda"]))
    .dropDuplicates(["item_id"])
)

print(f"✅ silver_items → {df_silver_items.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Silver Tables

# COMMAND ----------

# === SAVE SILVER ===
df_silver_orders.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.silver_orders")
df_silver_users.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.silver_users")
df_silver_items.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.silver_items")

print("=" * 60)
print("✅ Silver layer completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Results

# COMMAND ----------

print("📊 Silver Users with Targets:")
display(df_silver_users.orderBy(col("monthly_revenue_target").desc()))
