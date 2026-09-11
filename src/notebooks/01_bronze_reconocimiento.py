# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Bronze y reconocimiento de datos
# MAGIC
# MAGIC **Caso:** Cliente 360 — Riesgo de Churn y Oportunidad de Upsell  
# MAGIC **Objetivo:** Crear las cuatro tablas Bronze preservando el origen y realizar el perfilamiento inicial de calidad.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
SCHEMA_BRONZE = "l1_raw"
RUTA_LANDING = "/Volumes/claro_postpago/l1_raw/landing_files"

ARCHIVOS = {
    "dim_cliente": "dim_cliente.csv",
    "dim_producto": "dim_producto.csv",
    "fact_uso_servicio": "fact_uso_servicio.csv",
    "fact_pqr": "fact_pqr.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1. Ingesta Bronze
# MAGIC
# MAGIC Bronze preserva los datos fuente. Solo se agregan metadatos técnicos de trazabilidad.

# COMMAND ----------

def leer_bronze(nombre_archivo):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{RUTA_LANDING}/{nombre_archivo}")
        .withColumn("_source_file", F.lit(nombre_archivo))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )

# COMMAND ----------

for nombre_tabla, nombre_archivo in ARCHIVOS.items():
    df_bronze = leer_bronze(nombre_archivo)

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOGO}.{SCHEMA_BRONZE}.{nombre_tabla}_bronze")
    )

    print(
        f"Tabla creada: "
        f"{CATALOGO}.{SCHEMA_BRONZE}.{nombre_tabla}_bronze "
        f"| Registros: {df_bronze.count()}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento: esquema y muestra

# COMMAND ----------

TABLAS_BRONZE = [
    "dim_cliente_bronze",
    "dim_producto_bronze",
    "fact_uso_servicio_bronze",
    "fact_pqr_bronze",
]

for tabla in TABLAS_BRONZE:
    nombre_completo = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabla}"

    print(f"\n{'=' * 80}")
    print(nombre_completo)
    print(f"{'=' * 80}")

    df = spark.table(nombre_completo)
    print(f"Total de registros: {df.count()}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento: valores nulos

# COMMAND ----------

for tabla in TABLAS_BRONZE:
    nombre_completo = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabla}"
    df = spark.table(nombre_completo)

    nulos = df.select(
        [
            F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0))
            .alias(c)
            for c in df.columns
            if not c.startswith("_")
        ]
    )

    print(f"\nValores nulos o vacíos: {nombre_completo}")
    display(nulos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento: duplicados exactos

# COMMAND ----------

for tabla in TABLAS_BRONZE:
    nombre_completo = f"{CATALOGO}.{SCHEMA_BRONZE}.{tabla}"
    df = spark.table(nombre_completo)

    columnas_origen = [c for c in df.columns if not c.startswith("_")]
    total = df.count()
    distintos = df.select(*columnas_origen).dropDuplicates().count()

    print(
        f"{nombre_completo} | "
        f"Total: {total} | "
        f"Duplicados exactos: {total - distintos}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento: integridad referencial y valores fuera de rango

# COMMAND ----------

df_clientes = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.dim_cliente_bronze")
df_uso = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.fact_uso_servicio_bronze")
df_pqr = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.fact_pqr_bronze")

clientes_validos = df_clientes.select("id_cliente").dropDuplicates()

uso_huerfano = (
    df_uso.join(clientes_validos, "id_cliente", "left_anti")
)

consumo_negativo = (
    df_uso.filter(F.col("consumo_datos_gb").cast("double") < 0)
)

fecha_pqr_no_iso = (
    df_pqr.filter(
        F.to_date(F.col("fecha_apertura"), "yyyy-MM-dd").isNull()
    )
)

print("Usos con id_cliente inexistente:")
display(uso_huerfano)

print("Usos con consumo_datos_gb negativo:")
display(consumo_negativo)

print("PQR con fecha_apertura no ISO:")
display(fecha_pqr_no_iso)