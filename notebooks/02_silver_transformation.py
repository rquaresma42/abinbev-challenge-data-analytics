from pyspark.sql.functions import col, trim, lower, to_date, year, month

print("🔧 SILVER LAYER - Cleaning and Validation")
print("=" * 60)

# === SILVER ORDERS ===
df_silver_orders = (
    spark.table("abinbev.bronze.orders")
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("user_id", col("user_id").cast("long"))
    .withColumn("product_id", col("product_id").cast("long"))
    .withColumn("revenue", col("revenue").cast("double"))
    .dropDuplicates(["order_id", "product_id","user_id"])
    .filter(col("revenue") > 0)
    .filter(col("order_date").isNotNull())
    .select("order_id", "order_date", "user_id", "product_id", "revenue")  # Ensure schema match
)

print(f"✅ silver_orders → {df_silver_orders.count()} records")

# === SILVER USERS (MERGED WITH TARGETS) ===
df_users = spark.table("abinbev.bronze.users")
df_targets = spark.table("abinbev.bronze.targets")

df_silver_users = (
    df_users
    .withColumn("category", trim(lower(col("category"))))
    .join(
        df_targets.select(
            col("user_id").cast("long").alias("target_user_id"),
            trim(lower(col("category"))).alias("target_category"),
            trim(col("city")).alias("target_city"),
            col("monthly_revenue_target")
        ),
        on=[
            col("user_id") == col("target_user_id"),
            trim(lower(col("category"))) == col("target_category"),
            trim(col("city")) == col("target_city")
        ],
        how="left"
    )
    .select(
        col("user_id").cast("long"),
        col("category"),
        trim(col("city")).alias("city"),
        col("monthly_revenue_target").cast("double").alias("monthly_revenue_target")
    )
    # .dropDuplicates(["user_id", "category", "city"])
)

print(f"✅ silver_users → {df_silver_users.count()} records")

# === SILVER ITEMS (MERGED WITH TARGETS) ===
df_items = spark.table("abinbev.bronze.items")
df_targets = spark.table("abinbev.bronze.targets")

df_silver_items = (
    df_items
    .withColumn("category", trim(lower(col("category"))))
    .join(
        df_targets.select(
            col("user_id").cast("long").alias("target_user_id"),
            trim(lower(col("category"))).alias("target_category"),
            col("monthly_revenue_target")
        ),
        on=[
            col("category") == col("target_category")
        ],
        how="left"
    )
    .withColumn("item_id", col("item_id").cast("long"))
    .withColumn("monthly_revenue_target", col("monthly_revenue_target").cast("double"))
    .dropDuplicates(["item_id"])
    .select(
        "item_id",
        "category",
        "ingestion_timestamp",
        "source_file"
    )
)

print(f"✅ silver_items → {df_silver_items.count()} records")

# === SAVE SILVER ===
df_silver_orders.write.format("delta").mode("overwrite").saveAsTable("abinbev.silver.orders")
df_silver_users.write.format("delta").mode("overwrite").saveAsTable("abinbev.silver.users")
df_silver_items.write.format("delta").mode("overwrite").saveAsTable("abinbev.silver.items")

print("=" * 60)
print("✅ Silver layer completed!")