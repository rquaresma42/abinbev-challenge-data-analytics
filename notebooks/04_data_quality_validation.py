from pyspark.sql.functions import col, sum, count

print("🔍 DATA QUALITY VALIDATION")
print("=" * 60)

# === VALIDATE REFERENTIAL INTEGRITY ===
fact = spark.table("abinbev.gold.fact_orders")
dim_users = spark.table("abinbev.gold.dim_users")
dim_items = spark.table("abinbev.gold.dim_items")

# Check 1: Do all user_ids in fact exist in the dimension?
orphan_users = fact.join(dim_users, "user_id", "left_anti").select("user_id").distinct()
orphan_count = orphan_users.count()
print(f"{'✅' if orphan_count == 0 else '❌'} Orphan users: {orphan_count}")

# Check 2: Do all product_ids in fact exist in the dimension?
orphan_items = fact.join(
    dim_items,
    fact.product_id == dim_items.item_id,
    "left_anti"
).select("product_id").distinct()
orphan_items_count = orphan_items.count()
print(f"{'✅' if orphan_items_count == 0 else '❌'} Orphan items: {orphan_items_count}")

# Check 3: Is revenue always positive?
negative_revenue = fact.filter(col("revenue") <= 0).count()
print(f"{'✅' if negative_revenue == 0 else '❌'} Negative revenue: {negative_revenue}")

# Check 4: Valid dates?
invalid_dates = fact.filter(col("order_date").isNull()).count()
print(f"{'✅' if invalid_dates == 0 else '❌'} Invalid dates: {invalid_dates}")

print("\n📊 GOLD TABLES SUMMARY:")
print(f"   fact_orders: {fact.count():,} records")
print(f"   dim_users:   {dim_users.count():,} records")
print(f"   dim_items:   {dim_items.count():,} records")

# === QUICK EXPLORATORY ANALYSIS ===
print("\n💰 TOP 5 CLIENTS BY REVENUE:")
display(
    fact.groupBy("user_id")
    .agg(sum("revenue").alias("total_revenue"))
    .join(dim_users, "user_id")
    .select(
        "user_id",
        "city",
        "category",
        "total_revenue",
        (col("monthly_revenue_target") * 12).alias("annual_revenue_target")
    )
    .orderBy(col("total_revenue").desc())
    .limit(5)
)

print("\n📦 REVENUE BY PRODUCT CATEGORY:")
display(
    fact.join(dim_items, fact.product_id == dim_items.item_id)
    .groupBy("category")
    .agg(
        sum("revenue").alias("total_revenue"),
        count("*").alias("total_orders")
    )
    .orderBy(col("total_revenue").desc())
)

print("=" * 60)
print("✅ Validation completed!")