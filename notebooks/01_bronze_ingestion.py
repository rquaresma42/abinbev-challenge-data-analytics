# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer - Raw Data Ingestion
# MAGIC 
# MAGIC **Purpose**: Ingest raw tables from Databricks file upload into Bronze layer with minimal transformation.
# MAGIC 
# MAGIC **Input**: Raw tables created via Databricks UI data import
# MAGIC - `raw_items`
# MAGIC - `raw_orders`
# MAGIC - `raw_targets`
# MAGIC - `raw_users`
# MAGIC 
# MAGIC **Output**: Bronze Delta tables with ingestion timestamp
# MAGIC - `bronze_raw_items`
# MAGIC - `bronze_raw_orders`
# MAGIC - `bronze_raw_targets`
# MAGIC - `bronze_raw_users`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

print("📥 BRONZE LAYER - Table Ingestion")
print("=" * 60)

source_tables = ["raw_items", "raw_orders", "raw_targets", "raw_users"]

for table_name in source_tables:
    df = (
        spark.table(f"abinbev.default.{table_name}")
        .withColumn("ingestion_timestamp", current_timestamp())
    )
    
    # Rename columns with invalid characters (e.g., spaces)
    for col in df.columns:
        new_col = col.replace(" ", "_")
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    
    df.write.format("delta").mode("overwrite").saveAsTable(f"abinbev.default.bronze_{table_name}")
    
    record_count = df.count()
    print(f"✅ bronze_{table_name:10} → {record_count:5} records")

print("=" * 60)
print("✅ BRONZE layer completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC 
# MAGIC Verify that all Bronze tables were created successfully.

# COMMAND ----------

# Display sample from bronze_raw_orders
display(spark.table("abinbev.default.bronze_raw_orders").limit(5))
