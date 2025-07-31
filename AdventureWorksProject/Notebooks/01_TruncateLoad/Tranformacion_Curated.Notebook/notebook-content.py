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
from pyspark.sql.functions import col, current_timestamp


#------------------------------

# Leer archivo desde zona raw
df_sales_header = spark.read.format("parquet").load("Files/raw/sales/salesorderheader/SalesOrderHeader.parquet")

# Eliminar duplicados
df_sales_header = df_sales_header.dropDuplicates()

# Filtrar registros donde SalesOrderID o OrderDate estén vacíos
df_sales_header = df_sales_header.filter(
    col("SalesOrderID").isNotNull() & col("OrderDate").isNotNull()
)

# (Opcional) Eliminar columnas innecesarias
df_sales_header = df_sales_header.drop("rowguid", "Comment")

# Agregar columna de Fecha de Carga
df_sales_header = df_sales_header.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_sales_header.write.mode("overwrite").format("delta").save("Files/curated/sales/salesorderheader/SalesOrderHeader_curated")

#-----------------------
# Leer el archivo desde raw
df_customer = spark.read.format("parquet").load("Files/raw/sales/customer/Customer.parquet")

# Eliminar duplicados
df_customer = df_customer.dropDuplicates()

# Filtrar registros sin CustomerID
df_customer = df_customer.filter(col("CustomerID").isNotNull())

# (Opcional) Eliminar columnas innecesarias
df_customer = df_customer.drop("rowguid")

# Agregar columna FechaCarga
df_customer = df_customer.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_customer.write.mode("overwrite").format("delta").save("Files/curated/sales/customer/Customer_curated")

#----------------------------------------------

# Leer archivo desde zona raw
df_product = spark.read.format("parquet").load("Files/raw/production/product/Product.parquet")

# Eliminar duplicados
df_product = df_product.dropDuplicates()

# Filtrar registros sin ProductID o Name
df_product = df_product.filter(
    col("ProductID").isNotNull() & col("Name").isNotNull()
)

# (Opcional) Eliminar columnas que no necesitas
df_product = df_product.drop("rowguid")

# Agregar columna FechaCarga
df_product = df_product.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_product.write.mode("overwrite").format("delta").save("Files/curated/production/product/Product_curated")

##-----------------
# Leer archivo desde zona raw
df_productsubcategory = spark.read.format("parquet").load("Files/raw/production/productsubcategory/ProductSubcategory.parquet")

# Eliminar duplicados
df_productsubcategory = df_productsubcategory.dropDuplicates()

# Filtrar registros sin ProductSubcategoryID o Name
df_productsubcategory = df_productsubcategory.filter(
    col("ProductSubcategoryID").isNotNull() & col("Name").isNotNull()
)

# Eliminar columna innecesaria
df_productsubcategory = df_productsubcategory.drop("rowguid")

# Agregar columna FechaCarga
df_productsubcategory = df_productsubcategory.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_productsubcategory.write.mode("overwrite").format("delta").save("Files/curated/production/productsubcategory/ProductSubcategory_curated")




###-----------------------

# Leer archivo desde zona raw
df_productcategory = spark.read.format("parquet").load("Files/raw/production/productcategory/ProductCategory.parquet")

# Eliminar duplicados
df_productcategory = df_productcategory.dropDuplicates()

# Filtrar registros sin ProductCategoryID o Name
df_productcategory = df_productcategory.filter(
    col("ProductCategoryID").isNotNull() & col("Name").isNotNull()
)

# Eliminar columna innecesaria
df_productcategory = df_productcategory.drop("rowguid")

# Agregar columna FechaCarga
df_productcategory = df_productcategory.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_productcategory.write.mode("overwrite").format("delta").save("Files/curated/production/productcategory/ProductCategory_curated")

###-----------------------------------------

# Leer archivo desde zona raw
df_salesterritory = spark.read.format("parquet").load("Files/raw/sales/salesterritory/SalesTerritory.parquet")

# Eliminar duplicados
df_salesterritory = df_salesterritory.dropDuplicates()

# Filtrar registros sin TerritoryID o Name
df_salesterritory = df_salesterritory.filter(
    col("TerritoryID").isNotNull() & col("Name").isNotNull()
)

# Eliminar columna innecesaria
df_salesterritory = df_salesterritory.drop("rowguid")

# Agregar columna FechaCarga
df_salesterritory = df_salesterritory.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_salesterritory.write.mode("overwrite").format("delta").save("Files/curated/sales/salesterritory/SalesTerritory_curated")

##############------------------

# Leer archivo desde zona raw
df_salesperson = spark.read.format("parquet").load("Files/raw/sales/salesperson/SalesPerson.parquet")

# Eliminar duplicados
df_salesperson = df_salesperson.dropDuplicates()

# Filtrar registros sin BusinessEntityID
df_salesperson = df_salesperson.filter(
    col("BusinessEntityID").isNotNull()
)

# Eliminar columna innecesaria
df_salesperson = df_salesperson.drop("rowguid")

# Agregar columna FechaCarga
df_salesperson = df_salesperson.withColumn("FechaCarga", current_timestamp())

# Guardar en zona curated
df_salesperson.write.mode("overwrite").format("delta").save("Files/curated/sales/salesperson/SalesPerson_curated")








# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load("Files/curated/sales/customer/Customer_curated")
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
