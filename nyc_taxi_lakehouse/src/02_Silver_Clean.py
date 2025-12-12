# Databricks notebook source
# Next layer is -> Silver Layer Transformation
# DESCRIPTION: Cleans data and removes bad rows.

from pyspark.sql.functions import col, round, unix_timestamp

# 1. Read from the Bronze Table
print("Reading from Bronze Table...")
df_bronze = spark.read.table("bronze_taxi")

# 2. Clean Data -> Here it filter negative fares and zero distance)
df_clean = df_bronze.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0)
)

# 3. Feature Engineering 
df_silver = df_clean.withColumn(
    "trip_duration_mins", 
    round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
)

# 4. Save to Silver Table
print(f"Saving {df_silver.count()} rows to table 'silver_taxi'...")
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_taxi")

print("Successfully created Table 'silver_taxi'.")

# COMMAND ----------

