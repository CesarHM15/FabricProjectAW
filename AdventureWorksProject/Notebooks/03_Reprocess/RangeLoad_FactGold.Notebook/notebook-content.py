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

from pyspark.sql.functions import col, current_timestamp, lit, to_date

# 1. Convertir fechas a tipo date para el filtro
startDateCarga1 = to_date(lit(startDateCarga))
endDateCarga1 = to_date(lit(endDateCarga))

# 2. Cargar tablas silver
df_header = spark.read.format("delta").load("Files/curated/sales/salesorderheader/SalesOrderHeader_curated")
df_detail = spark.read.format("delta").load("Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated")

# 3. Filtrar tabla detail por rango de fechas
df_detail_filtered = df_detail.filter(
    (col("ModifiedDate") >= startDateCarga1) &
    (col("ModifiedDate") <= endDateCarga1)
)

# 4. Join con tabla header
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
        col("d.ModifiedDate"),  # ✅ Ya es tipo date
        current_timestamp().alias("LoadDate")
    )

# 5. Escribir en la capa gold con replaceWhere usando los strings originales
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ModifiedDate >= '{startDateCarga}' AND ModifiedDate <= '{endDateCarga}'") \
    .save("Files/gold/sales/SalesOrder/FactSalesOrder")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM delta.`Files/gold/sales/SalesOrder/FactSalesOrder`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from delta.`Files/curated/sales/SalesOrderDetail/SalesOrderDetail_Curated`

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "Files/gold/sales/SalesOrder/FactSalesOrder")
delta_table.history().show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("Files/gold/sales/SalesOrder/FactSalesOrder") \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("Files/gold/sales/SalesOrder/FactSalesOrder")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
