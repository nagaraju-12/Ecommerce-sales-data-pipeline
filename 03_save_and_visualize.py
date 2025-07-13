# Databricks notebook source
# MAGIC %md
# MAGIC ### saving the df_clean and df_summary as parquet format

# COMMAND ----------

#df_clean.write.mode("overwrite").parquet("/FileStore/tables/cleaned_data_parquet")
#df_summarry.write.mode("overwrite").parquet("/FileStore/tables/summary_data_parquet")

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, avg, to_date

# Load cleaned CSV data
df_clean = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/FileStore/tables/ecommerce_cleaned_data")

# Convert date strings to DateType
df_clean = df_clean.withColumn("OrderDate", to_date("OrderDate", "yyyy-MM-dd"))
df_clean = df_clean.withColumn("ShippingDate", to_date("ShippingDate", "yyyy-MM-dd"))

# Add Profit column (assuming cost = ₹20)
df_clean = df_clean.withColumn("Profit", col("Total_Sales") - col("Quantity") * 20)

# Show cleaned DataFrame
df_clean.display()

# Group and aggregate
df_summarry = df_clean.groupBy("Category").agg(
    sum("Total_Sales").alias("Total_Sales"),
    count("OrderID").alias("Total_orders"),
    sum("Profit").alias("Total_Profit"),
    avg("Quantity").alias("Avg_Quantity"),
    avg("Total_Sales").alias("Avg_Sales"),
    avg("Profit").alias("Avg_Profit")
)

# Show summary table
df_summarry.display()

# Save as Parquet
df_clean.write.mode("overwrite").parquet("/FileStore/tables/cleaned_data_parquet")
df_summarry.write.mode("overwrite").parquet("/FileStore/tables/summarry_data_parquet")



# COMMAND ----------

df_summarry.display()


# COMMAND ----------

# ✅ Job logging
print("✅ FINAL SAVE COMPLETE")
print("📁 Parquet File Saved: /FileStore/tables/summarry_data_parquet")

# Confirm saved file works
df_result = spark.read.parquet("/FileStore/tables/summarry_data_parquet")
print("📊 Saved Summarry Row Count:", df_result.count())
df_result.show()
