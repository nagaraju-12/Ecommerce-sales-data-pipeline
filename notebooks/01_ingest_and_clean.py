# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("EcommercePipeline")\
    .getOrCreate()
# COMMAND ----------

#df=spark.read.format("csv").option("header","True").option("inferSchema","True").load("dbfs:/FileStore/tables/ecommerce_raw_data.csv")
#df.display()

# COMMAND ----------

#df=df.fillna({'product':'Notavaliable','region':'Notavaliable','category':'Notavaliable'}).display()

# COMMAND ----------

from pyspark.sql.functions import col

# Step 1: Read the CSV file
df = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .load("dbfs:/FileStore/tables/ecommerce_raw_data.csv")

# Step 2: Fill nulls in specified columns
df = df.fillna({
    'product': 'Notavaliable',
    'region': 'Notavaliable',
    'category': 'Notavaliable'
})

# Step 3: Display after cleaning
df.display()

# Step 4: Sort by OrderID ascending
df = df.orderBy(col("OrderID").asc())

# Step 5: Final display
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###filtered

# COMMAND ----------

# Step 1: Filter out rows where Quantity and Total_Sales are > 0
df_clean = df.filter((col("Quantity") > 0) & (col("Total_Sales") > 0))

# Step 2: Display (separate line!)
df_clean.display()

# Step 3: Save cleaned data to CSV
#df_clean.write.format("csv") \.option("header", "True") \ .mode("overwrite") \.save("dbfs:/FileStore/tables/ecommerce_cleaned_data")



# COMMAND ----------
#dbutils.fs.ls("dbfs:/FileStore/tables/ecommerce_raw_data.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#from pyspark.sql.functions import col
#d_drop=df.dropna(subset=["Product", "Region", "Category"]).display()

# COMMAND ----------

# Step 1: Apply transformations
#d_drop = df.dropna()

# Step 2: Order the data
#d_drop = d_drop.orderBy("customerID")

# Step 3: Display
#d_drop.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### removal of duplicates

# COMMAND ----------

# Step 1: Remove duplicates
#if df1 is not None:    df1 = df1.dropDuplicates(['OrderID'])


# Step 2: Display the cleaned DataFrame
#if df1 is not None: df1.display()


# COMMAND ----------

#d_drop.dropDuplicates(["Quantity"]).display()

# COMMAND ----------

# ✅ Job logging
print("✅ CLEANING COMPLETE")
print("📊 Cleaned Data Row Count:", df_clean.count())
print("📦 Sample Cleaned Records:")
df_clean.show()
