from pyspark.sql.functions import *

print("⭐ GOLD LAYER - Star Schema Modeling")
print("=" * 60)

# === FACT_ORDERS ===
df_fact_orders = (
    spark.table("abinbev.silver.orders")
    .select("order_id", "order_date", "user_id", "product_id", "revenue")
)

print(f"✅ fact_orders → {df_fact_orders.count()} records")

# === DIM_USERS ===
df_dim_users = spark.table("abinbev.silver.users")
print(f"✅ dim_users → {df_dim_users.count()} records")

# === DIM_ITEMS ===
df_dim_items = spark.table("abinbev.silver.items")
print(f"✅ dim_items → {df_dim_items.count()} records")

# === SAVE GOLD ===
df_fact_orders.write.format("delta").mode("overwrite").saveAsTable("abinbev.gold.fact_orders")
df_dim_users.write.format("delta").mode("overwrite").saveAsTable("abinbev.gold.dim_users")
df_dim_items.write.format("delta").mode("overwrite").saveAsTable("abinbev.gold.dim_items")

print("=" * 60)
print("✅ Gold layer completed!")