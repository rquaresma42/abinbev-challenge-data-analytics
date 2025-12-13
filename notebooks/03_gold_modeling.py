# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer - Star Schema Modeling
# MAGIC 
# MAGIC **Purpose**: Create dimensional model (Star Schema) ready for analytics and Power BI.
# MAGIC 
# MAGIC **Star Schema Structure**:
# MAGIC - **Fact**: `fact_orders` (transactional sales data)
# MAGIC - **Dimensions**:
# MAGIC   - `dim_users` (customers with targets)
# MAGIC   - `dim_items` (products)
# MAGIC 
# MAGIC **Note**: Calendar dimension (`dim_calendar`) is created in Power BI using Power Query M.
# MAGIC 
# MAGIC **Input**: Silver tables
# MAGIC **Output**: Gold tables (fact and dimensions)

# COMMAND ----------

from pyspark.sql.functions import *

print("⭐ GOLD LAYER - Star Schema Modeling")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Orders

# COMMAND ----------

# === FACT_ORDERS ===
df_fact_orders = (
    spark.table("abinbev.default.silver_orders")
    .select("order_id", "order_date", "user_id", "product_id", "revenue", "year", "month")
)

print(f"✅ fact_orders → {df_fact_orders.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Users (with Targets)

# COMMAND ----------

# === DIM_USERS ===
df_dim_users = spark.table("abinbev.default.silver_users")
print(f"✅ dim_users → {df_dim_users.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Items (Products)

# COMMAND ----------

# === DIM_ITEMS ===
df_dim_items = spark.table("abinbev.default.silver_items")
print(f"✅ dim_items → {df_dim_items.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Gold Tables

# COMMAND ----------

# === SAVE GOLD ===
df_fact_orders.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.fact_orders")
df_dim_users.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.dim_users")
df_dim_items.write.format("delta").mode("overwrite").saveAsTable("abinbev.default.dim_items")

print("=" * 60)
print("✅ Gold layer completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star Schema Visualization
# MAGIC 
# MAGIC ```
# MAGIC       dim_users
# MAGIC           |
# MAGIC           | (user_id)
# MAGIC           |
# MAGIC     fact_orders ---- (product_id) ---- dim_items
# MAGIC           |
# MAGIC           | (order_date)
# MAGIC           |
# MAGIC     dim_calendar (created in Power BI)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Gold Tables

# COMMAND ----------

print("📊 Fact Orders Sample:")
display(df_fact_orders.limit(10))

print("\n📊 Dimension Users:")
display(df_dim_users)

print("\n📊 Dimension Items:")
display(df_dim_items)
