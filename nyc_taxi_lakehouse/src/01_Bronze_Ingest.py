# Databricks notebook source
# So this is "Bronze Layer Ingestion"
# DESCRIPTION: Ingests raw taxi data into a Managed Delta Table.

from pyspark.sql.functions import current_timestamp, lit

# 1. Here it can still READ from the public Databricks datasets
print("Raw Data Reading.....")
df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

# 2.Now we will add Audit Columns
df_bronze = df.withColumn("ingestion_time", current_timestamp())

# 3. Now we will lets Databricks decide where to store the data safely
print(f"Saving {df_bronze.count()} rows to table 'bronze_taxi'...")
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_taxi")

print("Successfully created Table 'bronze_taxi'.")

# COMMAND ----------

