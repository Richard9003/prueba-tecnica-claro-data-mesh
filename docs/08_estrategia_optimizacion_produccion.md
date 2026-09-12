# 08. Estrategia de optimización y operación productiva

## Alcance

La solución actual es un pipeline batch reproducible para el dominio Postpago Residencial. Su flujo es:

```text
CSV → Bronze Delta → Silver → Gold + Quality Gate → Analítica
```

La prueba se ejecuta con cuatro fuentes independientes y tablas Delta en Unity Catalog.

## Estructura de notebooks

| Orden | Notebook | Responsabilidad |
|---|---|---|
| 00 | `00_bootstrap_unity_catalog.py` | Crea catálogo, esquemas y Volume; ejecución inicial por ambiente |
| 01 | `01_bronze_dim_cliente.py` | CSV cliente → `dim_cliente_bronze` |
| 02 | `02_bronze_dim_producto.py` | CSV producto → `dim_producto_bronze` |
| 03 | `03_bronze_fact_uso_servicio.py` | CSV uso → `fact_uso_servicio_bronze` |
| 04 | `04_bronze_fact_pqr.py` | CSV PQR → `fact_pqr_bronze` |
| 05 | `05_bronze_reconocimiento_calidad.py` | Perfilamiento y validación de Bronze; no transforma Silver/Gold |
| 10 | `10_silver_dim_cliente.py` | Cliente Bronze → Cliente Silver |
| 11 | `11_silver_dim_producto.py` | Producto Bronze → Producto Silver |
| 12 | `12_silver_fact_uso_servicio.py` | Uso Silver y cuarentena |
| 13 | `13_silver_fact_pqr.py` | PQR Silver |
| 20 | `20_gold_cliente_360.py` | Producto Gold, reglas de negocio y Quality Gate |
| 21 | `21_analitica_p5.py` | Consulta analítica P5 |

La numeración separa claramente capas y evita que la organización física del repositorio dependa del orden de ejecución de una sola entidad.

## Dependencias del Job

```text
01 Bronze Cliente ─────┐
02 Bronze Producto ────┤
03 Bronze Uso ──────────┤→ 05 Reconocimiento → 10/11/12/13 Silver → 20 Gold → 21 Analítica
04 Bronze PQR ──────────┘
```

En producción, el reconocimiento puede ejecutarse como tarea de observabilidad. Si se desea que una anomalía bloquee el pipeline, debe devolver error únicamente ante fallas críticas de entrada, no ante los errores de calidad que el diseño debe enviar a cuarentena.

## Bronze

Cada notebook Bronze lee un único archivo desde:

```text
/Volumes/claro_postpago/l1_raw/landing_files/
```

y escribe una tabla Delta independiente. Bronze conserva los valores originales y agrega únicamente metadatos:

- `_source_file`.
- `_ingestion_timestamp`.
- `_pipeline_run_id`.
- `_record_hash`.

La separación permite reintentar una fuente sin repetir las demás. Los notebooks Silver continúan leyendo los mismos nombres de tablas, por lo que no cambia el contrato entre capas.

Antes de sobrescribir una tabla Bronze se valida que el archivo tenga columnas y al menos un registro. Si falla la lectura, se detiene esa tarea y no se reemplaza la tabla con datos vacíos.

## Silver y calidad

Se mantienen las reglas aprobadas:

- Cliente: eliminar duplicados exactos, ciudad nula como `No informado`, estrato nulo con mediana global y trazabilidad.
- Producto: tipado y extracción de `capacidad_gb` desde el nombre del producto.
- Uso: aislar referencias huérfanas, consumo negativo, días inválidos, incidencias inválidas y periodos inválidos en cuarentena.
- PQR: tipar columnas y estandarizar fechas ISO y `dd/MM/yyyy`.
- Las tablas derivadas se reconstruyen de forma determinista desde Bronze.

## Gold y Quality Gate

Gold contiene un registro por cliente activo. Uso y PQR se agregan por separado antes del join para evitar fan-out. Se mantiene:

- `churn_risk` con la ventana reproducible de cuatro meses basada en la fecha máxima disponible en PQR Silver.
- `upsell_flag` cuando el consumo promedio supera `1.80 × capacidad_gb`.
- Bloqueo si Gold está vacío, hay identificadores nulos o duplicados, riesgo inválido, consumo negativo o PQR abiertos mayores que PQR totales.
- Conservación de la última publicación válida cuando el Quality Gate falla.

## Orquestación

Se recomienda un Databricks Job con tareas independientes y dependencias explícitas:

1. Bronze Cliente, Producto, Uso y PQR en paralelo.
2. Reconocimiento/validación de entrada.
3. Silver Cliente y Producto en paralelo.
4. Silver Uso y Silver PQR según sus dependencias.
5. Gold y Quality Gate.
6. Analítica P5.

Configuración recomendada:

- Una sola ejecución concurrente del producto.
- Dos reintentos controlados por tarea.
- Alertas ante fallo del Job o incumplimiento del SLA.
- Schedule diario, con disponibilidad antes de las 08:00 hora Colombia.
- El bootstrap se ejecuta una vez por ambiente, no como tarea diaria.

## Evolución productiva

La versión de prueba usa `overwrite` porque los CSV representan el dataset completo y se necesita reproducibilidad. En producción se evolucionará a:

1. Landing inmutable por lote y fecha.
2. Registro de control con archivo, hash, tamaño, estado y conteos.
3. Ignorar archivos ya procesados mediante hash.
4. `MERGE` por llave de negocio: `id_cliente`, `id_producto`, `id_uso` e `id_caso`.
5. Watermark por lote o periodo.
6. Recalcular Gold completo o solo clientes impactados según volumen.

## Optimización

Para `fact_uso_servicio_silver` a gran escala:

- Liquid Clustering por `periodo`, `id_cliente` e `id_producto`, si está disponible.
- Como alternativa, particionar por `periodo` y aplicar `OPTIMIZE`/`ZORDER` según el patrón real de consulta.
- No particionar por `id_cliente` por su alta cardinalidad.
- Evaluar auto compaction, optimized writes y tamaño de archivos.
- Definir la retención de `VACUUM` según auditoría y time travel.

Estas optimizaciones no son necesarias para las 50 filas de la prueba.

## Versionamiento y cambios

Los notebooks, Jobs, parámetros y dependencias deben versionarse en Git. La modificación de una ruta se hace en la definición del Job o en Databricks Asset Bundles, no manualmente en cada consumidor. El cambio se valida con `databricks bundle validate`, pruebas y Pull Request antes del despliegue.

## Seguridad y gobierno

Unity Catalog administra catálogo, permisos, linaje y auditoría. Gold no publica `documento` ni `nombre_completo`; Mercadeo Digital consume una vista autorizada con mínimo privilegio.
