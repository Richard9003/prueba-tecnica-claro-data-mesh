# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Silver — Uso de Servicio y Cuarentena
# MAGIC
# MAGIC **Tratamiento aplicado:**
# MAGIC - Estandarizar llaves y tipos.
# MAGIC - Validar integridad referencial contra clientes y productos Silver.
# MAGIC - Validar consumo, días activos, incidencias y periodo.
# MAGIC - Enviar registros inválidos a cuarentena.
# MAGIC
# MAGIC Los registros válidos se publican en Silver. Los rechazados conservan
# MAGIC datos originales, motivo de rechazo y metadatos de trazabilidad.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
BRONZE = f"{CATALOGO}.l1_raw.fact_uso_servicio_bronze"
CLIENTES_SILVER = f"{CATALOGO}.l2_curated.dim_cliente_silver"
PRODUCTOS_SILVER = f"{CATALOGO}.l2_curated.dim_producto_silver"
SILVER = f"{CATALOGO}.l2_curated.fact_uso_servicio_silver"
CUARENTENA = f"{CATALOGO}.l2_curated.fact_uso_servicio_rechazados"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

df_bronze = spark.table(BRONZE)

df_clientes = (
    spark.table(CLIENTES_SILVER)
    .select("id_cliente")
    .dropDuplicates()
    .withColumn("_cliente_existe", F.lit(True))
)

df_productos = (
    spark.table(PRODUCTOS_SILVER)
    .select("id_producto")
    .dropDuplicates()
    .withColumn("_producto_existe", F.lit(True))
)

# COMMAND ----------

df_tipado = (
    df_bronze
    .select(
        F.upper(F.trim(F.col("id_uso"))).alias("id_uso"),
        F.upper(F.trim(F.col("id_cliente"))).alias("id_cliente"),
        F.upper(F.trim(F.col("id_producto"))).alias("id_producto"),
        F.trim(F.col("periodo")).alias("periodo"),
        F.expr("try_cast(consumo_datos_gb AS DOUBLE)").alias("consumo_datos_gb"),
        F.expr("try_cast(minutos_voz AS INT)").alias("minutos_voz"),
        F.expr("try_cast(sms_enviados AS INT)").alias("sms_enviados"),
        F.expr("try_cast(dias_activo_mes AS INT)").alias("dias_activo_mes"),
        F.expr("try_cast(incidencias_red AS INT)").alias("incidencias_red"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
        F.col("_pipeline_run_id"),
        F.col("_record_hash"),
    )
    .join(df_clientes, "id_cliente", "left")
    .join(df_productos, "id_producto", "left")
)

# COMMAND ----------

df_validado = df_tipado.withColumn(
    "motivo_rechazo",
    F.concat_ws(
        " | ",
        F.when(F.col("id_uso").isNull() | (F.trim(F.col("id_uso")) == ""), "ID_USO_NULO"),
        F.when(F.col("_cliente_existe").isNull(), "ID_CLIENTE_HUERFANO"),
        F.when(F.col("_producto_existe").isNull(), "ID_PRODUCTO_HUERFANO"),
        F.when(
            F.col("consumo_datos_gb").isNull()
            | (F.col("consumo_datos_gb") < 0),
            "CONSUMO_DATOS_INVALIDO",
        ),
        F.when(
            F.col("dias_activo_mes").isNull()
            | ~F.col("dias_activo_mes").between(0, 31),
            "DIAS_ACTIVO_MES_INVALIDO",
        ),
        F.when(
            F.col("incidencias_red").isNull()
            | (F.col("incidencias_red") < 0),
            "INCIDENCIAS_RED_INVALIDAS",
        ),
        F.when(
            ~F.col("periodo").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
            "PERIODO_INVALIDO",
        ),
    ),
)

df_rechazados = (
    df_validado
    .filter(F.col("motivo_rechazo") != "")
    .drop("_cliente_existe", "_producto_existe")
    .withColumn("_rejection_timestamp", F.current_timestamp())
    .withColumn("_rejection_run_id", F.lit(SILVER_RUN_ID))
)

df_uso_silver = (
    df_validado
    .filter(F.col("motivo_rechazo") == "")
    .drop("_cliente_existe", "_producto_existe", "motivo_rechazo")
    .dropDuplicates(["id_uso"])
    .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
    .withColumn("_silver_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    df_rechazados.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(CUARENTENA)
)

(
    df_uso_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

print(f"Silver listo | tabla={SILVER} | válidos={df_uso_silver.count()}")
print(f"Cuarentena lista | tabla={CUARENTENA} | rechazados={df_rechazados.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones de salida Silver

# COMMAND ----------

df_silver = spark.table(SILVER)

id_uso_nulo = df_silver.filter(
    F.col("id_uso").isNull()
    | (F.trim(F.col("id_uso")) == "")
)

id_uso_duplicado = (
    df_silver
    .groupBy("id_uso")
    .count()
    .filter(F.col("count") > 1)
)

consumo_invalido = df_silver.filter(
    F.col("consumo_datos_gb").isNull()
    | (F.col("consumo_datos_gb") < 0)
)

dias_invalidos = df_silver.filter(
    F.col("dias_activo_mes").isNull()
    | ~F.col("dias_activo_mes").between(0, 31)
)

incidencias_invalidas = df_silver.filter(
    F.col("incidencias_red").isNull()
    | (F.col("incidencias_red") < 0)
)

periodo_invalido = df_silver.filter(
    ~F.col("periodo").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
)

print(f"Validación | id_uso nulo={id_uso_nulo.count()}")
print(f"Validación | id_uso duplicado={id_uso_duplicado.count()}")
print(f"Validación | consumo nulo/negativo={consumo_invalido.count()}")
print(f"Validación | días activos inválidos={dias_invalidos.count()}")
print(f"Validación | incidencias inválidas={incidencias_invalidas.count()}")
print(f"Validación | periodo inválido={periodo_invalido.count()}")