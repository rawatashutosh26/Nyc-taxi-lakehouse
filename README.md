# 🚖 NYC Taxi Lakehouse Project

## 📌 Overview
An end-to-end Data Engineering pipeline built on **Azure Databricks (Free Edition)**. This project implements the **Medallion Architecture** (Bronze → Silver → Gold) to process NYC Taxi trip data, cleaning millions of records and calculating profitability metrics for business intelligence.

## 🛠 Tech Stack
* **Compute:** Databricks Serverless Compute (Spark 3.x)
* **Storage:** Managed Delta Tables (Data Lakehouse Pattern)
* **Language:** PySpark (Python) & SQL
* **Format:** Delta Lake (ACID Transactions)

## 🏗 Architecture
1.  **Bronze Layer (Ingestion):**
    * Ingested raw CSV data from the NYC Taxi public dataset.
    * Added audit columns (`ingestion_time`) for data lineage.
    * Stored as a **Managed Delta Table** (`bronze_taxi`).
2.  **Silver Layer (Transformation):**
    * Filtered corrupted data (negative fares, zero-distance trips).
    * Engineered new features: `trip_duration_minutes`.
    * Enforced data quality rules using PySpark.
3.  **Gold Layer (Analytics):**
    * Aggregated 7+ million rows to calculate **Total Revenue** and **Average Tip** per pickup zone.
    * Visualized top performing zones for business reporting.

## 📊 Results
*Top 20 Most Profitable Pickup Zones (Visualized in Databricks):*
<img width="1302" height="670" alt="Table" src="https://github.com/user-attachments/assets/9cd8cd5a-6824-462b-b408-1770f44007f1" />

<img width="1294" height="665" alt="gold_dashboard" src="https://github.com/user-attachments/assets/610f2e8a-1fde-4cc4-89c5-752a8dfafe51" />


## 🚀 How to Run
1.  Clone this repo.
2.  Import the files in `src/` to your Databricks Workspace.
3.  Run the notebooks in order: `01` → `02` → `03`.
