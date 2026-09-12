# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Bronze — Dimensión Cliente
# MAGIC
# MAGIC **Caso:** Cliente 360 — Riesgo de Churn y Oportunidad de Upsell  
# MAGIC **Alcance:** P1 Bronze — ingesta de CSV a Delta.
# MAGIC
# MAGIC **Tratamiento:** preservación del dato crudo con metadatos de trazabilidad.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
SCHEMA_BRONZE = "l1_raw"
RUTA_LANDING = "/Volumes/claro_postpago/l1_raw/landing_files"
PIPELINE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

ARCHIVO = "dim_cliente.csv"
TABLA = "dim_cliente_bronze"
LLAVE = "id_cliente"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesta Bronze

# COMMAND ----------

def leer_csv_bronze(nombre_archivo: str):
    df_origen = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(f"{RUTA_LANDING}/{nombre_archivo}")
    )

    columnas_origen = df_origen.columns

    return (
        df_origen
        .withColumn("_source_file", F.lit(nombre_archivo))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        .withColumn(
            "_record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                        for c in columnas_origen
                    ],
                ),
                256,
            ),
        )
    )

# COMMAND ----------

df_bronze = leer_csv_bronze(ARCHIVO)
tabla_destino = f"{CATALOGO}.{SCHEMA_BRONZE}.{TABLA}"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_destino)
)

print(
    f"Bronze listo | entidad=dim_cliente | "
    f"tabla={tabla_destino} | "
    f"registros={df_bronze.count()} | "
    f"run_id={PIPELINE_RUN_ID}"
)