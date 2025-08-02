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

# PARAMETERS CELL ********************

startDateCarga = ''
endDateCarga = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, current_timestamp, lit
from pyspark.sql import SparkSession


# 1. Load raw Parquet data
df_raw = spark.read.parquet("Files/raw/sales/salesorderdetail/SalesOrderDetail.parquet")

# 2. Cast columns
df_silver = df_raw \
    .withColumn("UnitPrice", col("UnitPrice").cast("decimal(18,2)")) \
    .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast("decimal(18,2)")) \
    .withColumn("LineTotal", col("LineTotal").cast("decimal(18,2)")) \
    .withColumn("ModifiedDate", to_date(col("ModifiedDate")))

# 3. Drop duplicates
df_silver = df_silver.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])

# 4. Filter invalid data
df_silver = df_silver \
    .filter(col("OrderQty") > 0) \
    .filter(col("UnitPrice") >= 0) \
    .filter(col("LineTotal") >= 0)

# 5. Add load timestamp
df_silver = df_silver.withColumn("LoadDate", current_timestamp())

# 6. Filter by parameterized dates
df_filtered = df_silver.filter(
    (col("ModifiedDate") >= lit(startDateCarga)) &
    (col("ModifiedDate") <= lit(endDateCarga))
)

# 7. Write to curated layer with replaceWhere
df_filtered.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ModifiedDate >= '{startDateCarga}' AND ModifiedDate <= '{endDateCarga}'") \
    .save("Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated")



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
