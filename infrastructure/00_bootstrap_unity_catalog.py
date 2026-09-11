# Databricks notebook source
# COMMAND ----------
# Inicializacion de recursos del dominio Postpago Residencial.
#
# Ejecutar una vez por ambiente antes de cargar fuentes.
# Las sentencias son idempotentes y no reemplazan recursos existentes.

CATALOG_NAME = "claro_postpago"

SCHEMAS = {
    "l1_raw": "l1_raw",
    "l2_curated": "l2_curated",
    "l3_certified": "l3_certified",
    "ops": "ops",
}

for layer, schema_name in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema_name}")
    print(f"Esquema disponible: {CATALOG_NAME}.{schema_name}")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.l1_raw.landing_files
COMMENT 'Archivos CSV de entrada del dominio Postpago Residencial'
""")

print(f"Volume disponible: /Volumes/{CATALOG_NAME}/l1_raw/landing_files/")