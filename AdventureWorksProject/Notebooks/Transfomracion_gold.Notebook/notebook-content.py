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


from pyspark.sql.functions import col


df_customer = spark.read.format("delta").load("Files/curated/sales/customer/Customer_curated")

df_dim_customer = (
    df_customer.select(
        "CustomerID",
        "PersonID",
        "StoreID",
        "TerritoryID",
        "AccountNumber"
    )
)

df_dim_customer.write.format("delta").mode("overwrite").save("Files/gold/sales/customer/dim_customer")
##################


df_salesperson = spark.read.format("delta").load("Files/curated/sales/salesperson/SalesPerson_curated")

df_dim_salesperson = (
    df_salesperson.select(
        col("BusinessEntityID").alias("SalesPersonID"),
        "TerritoryID",
        "SalesQuota",
        "Bonus",
        "CommissionPct",
        "SalesYTD",
        "SalesLastYear"
    )
)

df_dim_salesperson.write.format("delta").mode("overwrite").save("Files/gold/sales/salesperson/dim_salesperson")
##################


df_territory = spark.read.format("delta").load("Files/curated/sales/salesterritory/SalesTerritory_curated")

df_dim_territory = (
    df_territory.select(
        "TerritoryID",
        "Name",
        "CountryRegionCode",
        "Group"
    )
)

df_dim_territory.write.format("delta").mode("overwrite").save("Files/gold/sales/salesterritory/dim_territory")
#####################


# Cargar tablas
df_product = spark.read.format("delta").load("Files/curated/production/product/Product_curated")
df_subcat = spark.read.format("delta").load("Files/curated/production/productsubcategory/ProductSubcategory_curated")
df_cat = spark.read.format("delta").load("Files/curated/production/productcategory/ProductCategory_curated")

# Unir jerarquías
df_dim_product = (
    df_product
    .join(df_subcat, on="ProductSubcategoryID", how="left")
    .join(df_cat, on="ProductCategoryID", how="left")
    .select(
        "ProductID",
        df_product["Name"].alias("ProductName"),
        "ProductNumber",
        "Color",
        "StandardCost",
        "ListPrice",
        "Size",
        "Weight",
        df_subcat["Name"].alias("SubcategoryName"),
        df_cat["Name"].alias("CategoryName")
    )
)

df_dim_product.write.format("delta").mode("overwrite").save("Files/gold/production/productcategory/ProductCategory_curateddim_product")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
