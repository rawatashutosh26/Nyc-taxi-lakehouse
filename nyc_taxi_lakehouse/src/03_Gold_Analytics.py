# Databricks notebook source
# Final layer is -> Gold Layer Analytics
# DESCRIPTION: Business Intelligence aggregation.

# 1. Read from Silver Table
df_silver = spark.read.table("silver_taxi")

# 2. Run SQL Analysis -> Here it register a temp view to make writing SQL easier
df_silver.createOrReplaceTempView("taxi_view")

gold_df = spark.sql("""
    SELECT 
        PULocationID as pickup_zone_id,
        COUNT(*) as total_trips,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(AVG(tip_amount), 2) as avg_tip,
        SUM(total_amount) as total_revenue
    FROM taxi_view
    GROUP BY PULocationID
    ORDER BY total_revenue DESC
    LIMIT 20
""")

# 3. Save Final Gold Table
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_taxi_analytics")

# 4. Show the Report
print("Top 20 Most Profitable Zones:")
display(gold_df)

# COMMAND ----------

