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

# Databricks notebook: Silver to Gold - FactSalesOrder (Incremental from Detail only)
from pyspark.sql.functions import col, current_timestamp, current_date, date_sub

# 1. Load silver layer data
df_header = spark.read.format("delta").load("Files/curated/sales/salesorderheader/SalesOrderHeader_curated")
df_detail = spark.read.format("delta").load("Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated")

# 2. Filter detail table only (last 14 days)
df_detail_filtered = df_detail.filter(col("ModifiedDate") >= date_sub(current_date(), 14))

# 3. Join with full header table
# 3. Join with full header table
# 3. Join filtered detail with full header
df_fact = df_detail_filtered.alias("d") \
    .join(df_header.alias("h"), col("d.SalesOrderID") == col("h.SalesOrderID"), "inner") \
    .select(
    col("d.SalesOrderID"),
    col("SalesOrderDetailID"),
    col("ProductID"),
    col("OrderQty"),
    col("UnitPrice"),
    col("UnitPriceDiscount"),
    col("LineTotal"),
    col("CarrierTrackingNumber"),
    col("SpecialOfferID"),
    col("h.CustomerID"),
    col("h.SalesPersonID"),
    col("h.TerritoryID"),
    col("h.ShipMethodID"),
    col("h.CreditCardID"),
    col("h.OrderDate"),
    col("h.DueDate"),
    col("h.ShipDate"),
    col("h.Status"),
    col("h.CurrencyRateID"),
    col("h.SubTotal"),
    col("h.TaxAmt"),
    col("h.Freight"),
    col("d.ModifiedDate").cast("timestamp").alias("ModifiedDate"),  # ✅ cast aquí
    current_timestamp().alias("LoadDate")
)




# 4. Write only the affected dates (based on detail.ModifiedDate)
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "ModifiedDate >= DATE_SUB(CURRENT_DATE(), 14)") \
    .save("Files/gold/sales/SalesOrder/FactSalesOrder")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format("delta").load("Files/gold/sales/SalesOrder/FactSalesOrder").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(*) FROM delta.`Files/gold/sales/SalesOrder/FactSalesOrder`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
