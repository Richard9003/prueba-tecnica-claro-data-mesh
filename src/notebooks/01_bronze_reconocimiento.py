# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Bronze y reconocimiento de datos
# MAGIC
# MAGIC **Caso:** Cliente 360 — Riesgo de Churn y Oportunidad de Upsell  
# MAGIC **Alcance:** P1 Bronze y perfilamiento inicial para P2.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
SCHEMA_BRONZE = "l1_raw"
RUTA_LANDING = "/Volumes/claro_postpago/l1_raw/landing_files"
PIPELINE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

FUENTES = {
    "dim_cliente": {
        "archivo": "dim_cliente.csv",
        "tabla": "dim_cliente_bronze",
        "llave": "id_cliente",
    },
    "dim_producto": {
        "archivo": "dim_producto.csv",
        "tabla": "dim_producto_bronze",
        "llave": "id_producto",
    },
    "fact_uso_servicio": {
        "archivo": "fact_uso_servicio.csv",
        "tabla": "fact_uso_servicio_bronze",
        "llave": "id_uso",
    },
    "fact_pqr": {
        "archivo": "fact_pqr.csv",
        "tabla": "fact_pqr_bronze",
        "llave": "id_caso",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1. Ingesta Bronze
# MAGIC
# MAGIC Los CSV permanecen en Landing. Bronze se guarda como Delta sin limpieza funcional.
# MAGIC Solo se agregan metadatos de trazabilidad.

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

for entidad, config in FUENTES.items():
    df_bronze = leer_csv_bronze(config["archivo"])
    tabla_destino = f"{CATALOGO}.{SCHEMA_BRONZE}.{config['tabla']}"

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino)
    )

    print(
        f"Bronze listo | entidad={entidad} | "
        f"tabla={tabla_destino} | "
        f"registros={df_bronze.count()} | "
        f"run_id={PIPELINE_RUN_ID}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 1. Conteos, esquema y muestra

# COMMAND ----------

for entidad, config in FUENTES.items():
    tabla = f"{CATALOGO}.{SCHEMA_BRONZE}.{config['tabla']}"
    df = spark.table(tabla)

    print(f"\n{'=' * 90}")
    print(f"ENTIDAD: {entidad} | TABLA: {tabla}")
    print(f"TOTAL REGISTROS: {df.count()}")
    print(f"{'=' * 90}")

    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 2. Nulos o vacíos por columna

# COMMAND ----------

for entidad, config in FUENTES.items():
    tabla = f"{CATALOGO}.{SCHEMA_BRONZE}.{config['tabla']}"
    df = spark.table(tabla)
    columnas_origen = [c for c in df.columns if not c.startswith("_")]

    resumen_nulos = df.select(
        [
            F.sum(
                F.when(
                    F.col(c).isNull() | (F.trim(F.col(c)) == ""),
                    1,
                ).otherwise(0)
            ).alias(c)
            for c in columnas_origen
        ]
    )

    print(f"\nNULOS O VACÍOS | {entidad}")
    display(resumen_nulos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 3. Duplicados exactos y por llave

# COMMAND ----------

for entidad, config in FUENTES.items():
    tabla = f"{CATALOGO}.{SCHEMA_BRONZE}.{config['tabla']}"
    llave = config["llave"]
    df = spark.table(tabla)
    columnas_origen = [c for c in df.columns if not c.startswith("_")]

    total = df.count()
    total_unico = df.select(*columnas_origen).dropDuplicates().count()
    duplicados_exactos = total - total_unico

    duplicados_llave = (
        df.groupBy(llave)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.desc("count"), F.asc(llave))
    )

    print(
        f"\nDUPLICADOS | entidad={entidad} | "
        f"exactos={duplicados_exactos} | "
        f"llaves_duplicadas={duplicados_llave.count()}"
    )
    display(duplicados_llave)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 4. Integridad referencial

# COMMAND ----------

df_clientes = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.dim_cliente_bronze")
df_productos = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.dim_producto_bronze")
df_uso = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.fact_uso_servicio_bronze")
df_pqr = spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.fact_pqr_bronze")

clientes = df_clientes.select("id_cliente").dropDuplicates()
productos = df_productos.select("id_producto").dropDuplicates()

uso_cliente_huerfano = df_uso.join(clientes, "id_cliente", "left_anti")
uso_producto_huerfano = df_uso.join(productos, "id_producto", "left_anti")
pqr_cliente_huerfano = df_pqr.join(clientes, "id_cliente", "left_anti")

print(f"\nUSO → CLIENTE | huérfanos={uso_cliente_huerfano.count()}")
display(uso_cliente_huerfano)

print(f"\nUSO → PRODUCTO | huérfanos={uso_producto_huerfano.count()}")
display(uso_producto_huerfano)

print(f"\nPQR → CLIENTE | huérfanos={pqr_cliente_huerfano.count()}")
display(pqr_cliente_huerfano)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 5. Rangos y formatos

# COMMAND ----------

estrato_numerico = F.expr("try_cast(estrato AS DOUBLE)")
consumo_numerico = F.expr("try_cast(consumo_datos_gb AS DOUBLE)")
dias_activos_numerico = F.expr("try_cast(dias_activo_mes AS INT)")
incidencias_numerico = F.expr("try_cast(incidencias_red AS INT)")
dias_resolucion_numerico = F.expr("try_cast(dias_resolucion AS INT)")
satisfaccion_numerica = F.expr("try_cast(satisfaccion_1_5 AS INT)")

fecha_pqr_iso = F.expr("try_to_date(fecha_apertura, 'yyyy-MM-dd')")
fecha_pqr_dmy = F.expr("try_to_date(fecha_apertura, 'dd/MM/yyyy')")

clientes_estrato_invalido = df_clientes.filter(
    F.col("estrato").isNotNull()
    & (F.trim(F.col("estrato")) != "")
    & ~estrato_numerico.between(1, 6)
)

uso_consumo_negativo = df_uso.filter(consumo_numerico < 0)

uso_dias_invalidos = df_uso.filter(~dias_activos_numerico.between(0, 31))

uso_incidencias_invalidas = df_uso.filter(incidencias_numerico < 0)

periodo_invalido = df_uso.filter(
    ~F.col("periodo").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
)

pqr_dias_invalidos = df_pqr.filter(dias_resolucion_numerico < 0)

pqr_satisfaccion_invalida = df_pqr.filter(
    ~satisfaccion_numerica.between(1, 5)
)

pqr_fecha_no_iso = df_pqr.filter(fecha_pqr_iso.isNull())

pqr_fecha_no_parseable = df_pqr.filter(
    fecha_pqr_iso.isNull() & fecha_pqr_dmy.isNull()
)

validaciones = [
    ("dim_cliente | estrato fuera de 1-6", clientes_estrato_invalido),
    ("fact_uso_servicio | consumo negativo", uso_consumo_negativo),
    ("fact_uso_servicio | días activos fuera de 0-31", uso_dias_invalidos),
    ("fact_uso_servicio | incidencias negativas", uso_incidencias_invalidas),
    ("fact_uso_servicio | periodo inválido", periodo_invalido),
    ("fact_pqr | días resolución negativos", pqr_dias_invalidos),
    ("fact_pqr | satisfacción fuera de 1-5", pqr_satisfaccion_invalida),
    ("fact_pqr | fecha no ISO", pqr_fecha_no_iso),
    ("fact_pqr | fecha no parseable", pqr_fecha_no_parseable),
]

for nombre_regla, df_resultado in validaciones:
    print(f"\n{nombre_regla} | registros={df_resultado.count()}")
    display(df_resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 6. Dominios categóricos

# COMMAND ----------

DOMINIOS = [
    ("dim_cliente", df_clientes, "tipo_documento"),
    ("dim_cliente", df_clientes, "segmento"),
    ("dim_cliente", df_clientes, "estado_cliente"),
    ("dim_cliente", df_clientes, "canal_adquisicion"),
    ("dim_producto", df_productos, "tipo_producto"),
    ("dim_producto", df_productos, "tecnologia"),
    ("dim_producto", df_productos, "vigente"),
    ("fact_pqr", df_pqr, "tipo_caso"),
    ("fact_pqr", df_pqr, "motivo"),
    ("fact_pqr", df_pqr, "canal"),
    ("fact_pqr", df_pqr, "estado_caso"),
]

for entidad, df, columna in DOMINIOS:
    print(f"\nDOMINIO | entidad={entidad} | columna={columna}")
    display(
        df.groupBy(columna)
        .count()
        .orderBy(F.desc("count"), F.asc(columna))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconocimiento 7. Perfilamiento numérico y temporal

# COMMAND ----------

fecha_alta = F.expr("try_to_date(fecha_alta, 'yyyy-MM-dd')")
fecha_pqr_estandar = F.coalesce(fecha_pqr_iso, fecha_pqr_dmy)

display(
    df_clientes.select(
        F.min(fecha_alta).alias("fecha_alta_min"),
        F.max(fecha_alta).alias("fecha_alta_max"),
        F.min(estrato_numerico).alias("estrato_min"),
        F.max(estrato_numerico).alias("estrato_max"),
    )
)

display(
    df_uso.select(
        F.min(consumo_numerico).alias("consumo_min_gb"),
        F.max(consumo_numerico).alias("consumo_max_gb"),
        F.min(dias_activos_numerico).alias("dias_activo_min"),
        F.max(dias_activos_numerico).alias("dias_activo_max"),
        F.min(incidencias_numerico).alias("incidencias_min"),
        F.max(incidencias_numerico).alias("incidencias_max"),
    )
)

display(
    df_pqr.select(
        F.min(fecha_pqr_estandar).alias("fecha_apertura_min"),
        F.max(fecha_pqr_estandar).alias("fecha_apertura_max"),
        F.min(satisfaccion_numerica).alias("satisfaccion_min"),
        F.max(satisfaccion_numerica).alias("satisfaccion_max"),
    )
)