from pyspark.sql.functions import current_timestamp, lit
import pandas as pd
import os

VOLUME_PATH = "/Volumes/abinbev/bronze/excel-files"

print("📂 BRONZE LAYER - Excel to Delta")
print("=" * 60)

files = [f for f in os.listdir(VOLUME_PATH) if f.endswith('.xlsx')]

for filename in files:
    table_name = os.path.splitext(filename)[0]
    print(f"\n📥 Processing: {filename}")
    
    # Read Excel directly from volume path
    pandas_df = pd.read_excel(f"{VOLUME_PATH}/{filename}", engine='openpyxl')
    spark_df = spark.createDataFrame(pandas_df)
    
    # Clean column names
    for col in spark_df.columns:
        clean = col.strip().replace(" ", "_").lower()
        if clean != col:
            spark_df = spark_df.withColumnRenamed(col, clean)
    
    # Add audit columns
    spark_df = (
        spark_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(f"{VOLUME_PATH}/{filename}"))
    )
    
    # Save as Delta
    spark_df.write.format("delta").mode("overwrite").saveAsTable(f"abinbev.bronze.{table_name}")
    
    print(f"✅ bronze_{table_name} → {spark_df.count()} records")

print("\n" + "=" * 60)
print("✅ BRONZE layer completed!")