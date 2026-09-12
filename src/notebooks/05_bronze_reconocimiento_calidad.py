# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Bronze — Reconocimiento y calidad de entrada
# MAGIC Este notebook no transforma ni publica Silver/Gold. Solo perfila las cuatro tablas Bronze.

# COMMAND ----------
from pyspark.sql import functions as F

CATALOGO = "claro_postpago"
SCHEMA = "l1_raw"
FUENTES = {
    "dim_cliente": ("dim_cliente_bronze", "id_cliente"),
    "dim_producto": ("dim_producto_bronze", "id_producto"),
    "fact_uso_servicio": ("fact_uso_servicio_bronze", "id_uso"),
    "fact_pqr": ("fact_pqr_bronze", "id_caso"),
}

# COMMAND ----------
for entidad, (tabla_nombre, llave) in FUENTES.items():
    tabla = f"{CATALOGO}.{SCHEMA}.{tabla_nombre}"
    df = spark.table(tabla)
    columnas = [c for c in df.columns if not c.startswith("_")]
    total = df.count()
    unicos = df.select(*columnas).dropDuplicates().count()
    llaves_duplicadas = df.groupBy(llave).count().filter(F.col("count") > 1).count()
    print(f"{entidad} | registros={total} | duplicados_exactos={total-unicos} | llaves_duplicadas={llaves_duplicadas}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------
# Validaciones de calidad y referencialidad
clientes = spark.table(f"{CATALOGO}.{SCHEMA}.dim_cliente_bronze").select("id_cliente").dropDuplicates()
productos = spark.table(f"{CATALOGO}.{SCHEMA}.dim_producto_bronze").select("id_producto").dropDuplicates()
uso = spark.table(f"{CATALOGO}.{SCHEMA}.fact_uso_servicio_bronze")
pqr = spark.table(f"{CATALOGO}.{SCHEMA}.fact_pqr_bronze")

print(f"Uso -> cliente huérfano={uso.join(clientes, 'id_cliente', 'left_anti').count()}")
print(f"Uso -> producto huérfano={uso.join(productos, 'id_producto', 'left_anti').count()}")
print(f"PQR -> cliente huérfano={pqr.join(clientes, 'id_cliente', 'left_anti').count()}")

consumo = F.expr("try_cast(consumo_datos_gb AS DOUBLE)")
fecha_iso = F.expr("try_to_date(fecha_apertura, 'yyyy-MM-dd')")
fecha_dmy = F.expr("try_to_date(fecha_apertura, 'dd/MM/yyyy')")

reglas = [
    ("consumo_negativo", uso.filter(consumo < 0)),
    ("periodo_invalido", uso.filter(~F.col("periodo").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$"))),
    ("fecha_pqr_no_parseable", pqr.filter(fecha_iso.isNull() & fecha_dmy.isNull())),
    ("satisfaccion_fuera_1_5", pqr.filter(~F.expr("try_cast(satisfaccion_1_5 AS INT)").between(1, 5))),
]

for nombre, resultado in reglas:
    print(f"{nombre}={resultado.count()}")
    display(resultado)
