# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "68b8a2ba-a9ac-4ace-bdce-77b79857ba45",
# META       "default_lakehouse_name": "Lakehouse_AdventureWorks2019",
# META       "default_lakehouse_workspace_id": "a391470e-c4c7-4f69-a85d-516c1ef6018a",
# META       "known_lakehouses": [
# META         {
# META           "id": "68b8a2ba-a9ac-4ace-bdce-77b79857ba45"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Databricks notebook: Raw to Silver - SalesOrderDetail
from pyspark.sql.functions import col, to_date, lit, when, current_date, current_timestamp

# 1. Load raw Parquet data
df_raw = spark.read.parquet("Files/raw/sales/salesorderdetail/SalesOrderDetail.parquet")

# 2. Cast columns to correct types
df_silver = df_raw \
    .withColumn("UnitPrice", col("UnitPrice").cast("decimal(18,2)")) \
    .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast("decimal(18,2)")) \
    .withColumn("LineTotal", col("LineTotal").cast("decimal(18,2)")) \
    .withColumn("ModifiedDate", to_date(col("ModifiedDate")))

# 3. Drop duplicates based on composite key
df_silver = df_silver.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])

# 4. Filter invalid data (e.g., negative quantities or prices)
df_silver = df_silver \
    .filter(col("OrderQty") > 0) \
    .filter(col("UnitPrice") >= 0) \
    .filter(col("LineTotal") >= 0)

# 5. Add load timestamp
df_silver = df_silver.withColumn("LoadDate", current_timestamp())

# 6. Write to Silver layer (Delta format recommended)
df_silver.write.mode("overwrite").format("delta").save("Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM delta.`Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated`


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
