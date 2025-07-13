#E-commerce Sales Data Pipeline (PySpark on Databricks)

This project builds a complete end-to-end data pipeline using PySpark on Databricks to process and analyze e-commerce sales data.

## Objectives

- Ingest raw e-commerce data from CSV
- Clean and transform data using PySpark
- Add derived metrics like Profit
- Group and summarize sales by category
- Save cleaned and summary outputs as Parquet
- Automate pipeline using Databricks Jobs

## Tech Stack

-  PySpark
-  Databricks (Community Edition)
-  Spark SQL
-  Parquet Format
-  DBFS (Databricks File System)

## Folder Structure
----------------------

## Pipeline Overview

1. **Ingest & Clean:**
   - Read raw CSV
   - Drop nulls, fill missing values
   - Remove duplicates
   - Filter invalid records

2. **Transform & Aggregate:**
   - Add 'Profit' column (Sales - Cost)
   - Convert date columns
   - Group by Category and calculate:
     - Total Sales
     - Total Orders
     - Total Profit
     - Avg Quantity
     - Avg Profit

3. **Save & Visualize:**
   - Save outputs as Parquet
   - Visualize in Databricks
   - Optionally create dashboards


## Run Instructions

1. Upload the 'ecommerce_raw_data.csv' to DBFS:  
   '/FileStore/tables/ecommerce_raw_data.csv'

2. Run the notebooks in order:
   - '01_ingest_and_clean.ipynb'
   - '02_transform_and_aggregate.ipynb'
   - '03_save_and_visualize.ipynb'

3. (Optional) Create a Job Workflow with 3 tasks in Databricks.

## Output Location

- Cleaned Data:  
  'dbfs:/FileStore/tables/cleaned_data_parquet/'

- Summary Data:  
  'dbfs:/FileStore/tables/summary_data_parquet/'




