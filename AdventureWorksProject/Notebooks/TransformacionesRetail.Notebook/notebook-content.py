# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6875b41a-7300-4929-b1f7-c5aae55c728e",
# META       "default_lakehouse_name": "RetailLakehouse",
# META       "default_lakehouse_workspace_id": "a391470e-c4c7-4f69-a85d-516c1ef6018a",
# META       "known_lakehouses": [
# META         {
# META           "id": "6875b41a-7300-4929-b1f7-c5aae55c728e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Asegúrate de que el notebook esté conectado al Lakehouse 'RetailLakehouse'
# Kernel: PySpark

# =============================
# Paso 1: Leer tablas desde la zona raw
# =============================

df_clientes = spark.read.table("clientes")
df_productos = spark.read.table("productos")
df_ventas = spark.read.table("ventas")

# =============================
# Paso 2: Renombrar columnas para evitar duplicados
# =============================

df_clientes_renamed = df_clientes.withColumnRenamed("nombre", "nombre_cliente")
df_productos_renamed = df_productos.withColumnRenamed("nombre", "nombre_producto")

# =============================
# Paso 3: Transformaciones
# =============================

from pyspark.sql.functions import col, lit

# Filtrar ventas mayores a 5000
ventas_filtradas = df_ventas.filter(col("total") > 5000)

# Enriquecer con info de cliente y producto
ventas_enriquecidas = ventas_filtradas \
    .join(df_clientes_renamed, "cliente_id", "left") \
    .join(df_productos_renamed, "producto_id", "left") \
    .withColumn("categoria_mayus", col("categoría").cast("string")) \
    .withColumn("descuento_aplicado", lit(False))

# =============================
# Paso 4: Guardar resultados en carpeta curated con overwriteSchema
# =============================

ventas_filtradas.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true") \
    .save("Files/curated/ventas_curadas")

df_productos_renamed.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true") \
    .save("Files/curated/productos_curados")

df_clientes_renamed.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true") \
    .save("Files/curated/clientes_curados")

ventas_enriquecidas.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true") \
    .save("Files/curated/ventas_enriquecidas")

# =============================
# Paso 5 (opcional): Registrar como tablas SQL en el Lakehouse
# =============================

# Leer desde carpeta y guardar como tabla gestionada (managed table)
ventas_enriquecidas.write.mode("overwrite").format("delta").saveAsTable("ventas_enriquecidas")
productos_curados.write.mode("overwrite").format("delta").saveAsTable("productos_curados")
clientes_curados.write.mode("overwrite").format("delta").saveAsTable("clientes_curados")
ventas_curadas.write.mode("overwrite").format("delta").saveAsTable("ventas_curadas")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
