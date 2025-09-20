# Databricks notebook source
from pyspark.sql.functions import col, sum, count, avg, to_date

# Step 1: Load cleaned data from the previous step
df_clean = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/FileStore/tables/ecommerce_cleaned_data")
# Step 2: Convert string date columns to actual DateType
df_clean = df_clean.withColumn("OrderDate", to_date("OrderDate", "yyyy-MM-dd"))
df_clean = df_clean.withColumn("ShippingDate", to_date("ShippingDate", "yyyy-MM-dd"))
df_clean=df_clean.withColumn("Profit",col("Total_Sales")-col("Quantity")*20)
df_clean.display()
df_summarry=df_clean.groupBy("Category").agg(sum("Total_Sales").alias("Total_Sales"),count("OrderID").alias("Total_orders"),sum("Profit").alias("Total_Profit"),
        avg("Quantity").alias("Avg_Quantity"),avg("Total_Sales").alias("Avg_Sales"),avg("Profit").alias("Avg_Profit"))
df_summarry.display()

# COMMAND ----------

#  Job logging
print(" TRANSFORMATION COMPLETE")
print(" Summarry Row Count:", df_summarry.count())
print(" Sample Aggregated Records:")
df_summarry.show()
