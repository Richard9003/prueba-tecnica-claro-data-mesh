# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Silver — PQR
# MAGIC
# MAGIC **Tratamiento:**
# MAGIC - Estandarizar fechas (YYYY-MM-dd o dd/MM/yyyy).
# MAGIC - Eliminar duplicados por `id_caso`.
# MAGIC - Estandarizar texto.
# MAGIC
# MAGIC **Constraints (despu és de crear la tabla):**
# MAGIC - `id_caso` NOT NULL y ÚNICO.
# MAGIC - `satisfaccion_1_5` BETWEEN 1 AND 5 (si no es NULL).
# MAGIC - `dias_resolucion` >= 0.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.fact_pqr_bronze"
SILVER = f"{CATALOGO}.l2_curated.fact_pqr_silver"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

def limpiar_texto(columna):
    return F.initcap(F.regexp_replace(F.trim(F.col(columna)), r"\s+", " "))

fecha_apertura_estandar = F.coalesce(
    F.expr("try_to_date(fecha_apertura, 'yyyy-MM-dd')"),
    F.expr("try_to_date(fecha_apertura, 'dd/MM/yyyy')"),
)

# COMMAND ----------

df_bronze = spark.table(BRONZE)

df_pqr_silver = (
    df_bronze
    .select(
        F.upper(F.trim(F.col("id_caso"))).alias("id_caso"),
        F.upper(F.trim(F.col("id_cliente"))).alias("id_cliente"),
        limpiar_texto("tipo_caso").alias("tipo_caso"),
        limpiar_texto("motivo").alias("motivo"),
        fecha_apertura_estandar.alias("fecha_apertura"),
        limpiar_texto("canal").alias("canal"),
        F.expr("try_cast(dias_resolucion AS INT)").alias("dias_resolucion"),
        F.expr("try_cast(satisfaccion_1_5 AS INT)").alias("satisfaccion_1_5"),
        limpiar_texto("estado_caso").alias("estado_caso"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
        F.lit(SILVER_RUN_ID).alias("_silver_run_id"),
        F.current_timestamp().alias("_silver_timestamp"),
    )
    .dropDuplicates(["id_caso"])
)

# COMMAND ----------

(
    df_pqr_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"Silver listo | tabla={SILVER} | registros={df_pqr_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar constraints en la tabla Delta

# COMMAND ----------

# Constraint: id_caso NOT NULL
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT id_caso_not_null
    EXPECT (id_caso IS NOT NULL)
""")

# Constraint: satisfaccion_1_5 BETWEEN 1 AND 5 (si no es NULL)
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT satisfaccion_valida
    EXPECT (satisfaccion_1_5 IS NULL OR satisfaccion_1_5 BETWEEN 1 AND 5)
""")

# Constraint: dias_resolucion >= 0
spark.sql(f"""
    ALTER TABLE {SILVER}
    ADD CONSTRAINT dias_resolucion_no_negativo
    EXPECT (dias_resolucion >= 0)
""")

print("Constraints aplicados en fact_pqr_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_caso_nulo = df_silver.filter(
    F.col("id_caso").isNull()
    | (F.trim(F.col("id_caso")) == "")
)

id_caso_duplicado = (
    df_silver
    .groupBy("id_caso")
    .count()
    .filter(F.col("count") > 1)
)

satisfaccion_invalida = df_silver.filter(
    (F.col("satisfaccion_1_5").isNotNull()) &
    (~F.col("satisfaccion_1_5").between(1, 5))
)

dias_resolucion_negativo = df_silver.filter(
    F.col("dias_resolucion") < 0
)

print(f"Validaci ón | id_caso nulo={id_caso_nulo.count()}")
print(f"Validaci ón | id_caso duplicado={id_caso_duplicado.count()}")
print(f"Validaci ón | satisfacci ón inv álida={satisfaccion_invalida.count()}")
print(f"Validaci ón | d ías resoluci ón negativo={dias_resolucion_negativo.count()}")