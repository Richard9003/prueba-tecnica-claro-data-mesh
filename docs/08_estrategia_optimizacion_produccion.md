# 08. Estrategia de optimizacion y operacion productiva


## Alcance


La solucion actual es un pipeline batch reproducible para el dominio Postpago Residencial. Su flujo es:


```text
CSV → Bronze Delta → Silver → Gold + Quality Gate → Analitica
```


La prueba se ejecuta con cuatro fuentes independientes y tablas Delta en Unity Catalog.


## Estructura de notebooks


| Orden | Notebook | Responsabilidad |
|---|---|---|
| 00 | `00_bootstrap_unity_catalog.py` | Crea catalogo, esquemas y Volume; ejecucion inicial por ambiente |
| 01 | `01_bronze_dim_cliente.py` | CSV cliente → `dim_cliente_bronze` |
| 02 | `02_bronze_dim_producto.py` | CSV producto → `dim_producto_bronze` |
| 03 | `03_bronze_fact_uso_servicio.py` | CSV uso → `fact_uso_servicio_bronze` |
| 04 | `04_bronze_fact_pqr.py` | CSV PQR → `fact_pqr_bronze` |
| 05 | `05_bronze_reconocimiento_calidad.py` | Perfilamiento y validacion de Bronze; no transforma Silver/Gold |
| 10 | `10_silver_dim_cliente.py` | Cliente Bronze → Cliente Silver |
| 11 | `11_silver_dim_producto.py` | Producto Bronze → Producto Silver |
| 12 | `12_silver_fact_uso_servicio.py` | Uso Silver y cuarentena |
| 13 | `13_silver_fact_pqr.py` | PQR Silver |
| 20 | `20_gold_cliente_360.py` | Producto Gold, reglas de negocio y Quality Gate |
| 21 | `21_analitica_p5.py` | Consulta analitica P5 |


La numeracion separa claramente capas y evita que la organizacion fisica del repositorio dependa del orden de ejecucion de una sola entidad.


## Dependencias del Job


```text
01 Bronze Cliente ─────┐
02 Bronze Producto ────┤
03 Bronze Uso ──────────┤→ 05 Reconocimiento → 10/11/12/13 Silver → 20 Gold → 21 Analitica
04 Bronze PQR ──────────┘
```


En produccion, el reconocimiento puede ejecutarse como tarea de observabilidad. Solo debe fallar el pipeline ante errores criticos de ingesta (archivo ausente, esquema incompatible), no ante los hallazgos de calidad que el diseño envia a cuarentena.


## Bronze


Cada notebook Bronze lee un unico archivo desde:


```text
/Volumes/claro_postpago/l1_raw/landing_files/
```


y escribe una tabla Delta independiente. Bronze conserva los valores originales y agrega metadatos de trazabilidad:


- `_source_file`.
- `_ingestion_timestamp`.
- `_pipeline_run_id`.
- `_record_hash`.


La separacion permite reintentar una fuente sin repetir las demas. Los notebooks Silver continuan leyendo los mismos nombres de tablas, por lo que no cambia el contrato entre capas.


Antes de sobrescribir una tabla Bronze se valida que el archivo tenga las columnas esperadas y al menos un registro. Si falla la lectura, se detiene esa tarea y no se reemplaza la tabla con datos vacios.


## Silver y calidad


Se aplican las reglas definidas en el perfilamiento de Bronze:


- Cliente: eliminar duplicados exactos, ciudad nula como `No informado`, estrato nulo imputado con mediana global y trazabilidad del tratamiento.
- Producto: tipado correcto y extraccion de `capacidad_gb` desde el nombre del producto.
- Uso: aislar referencias huerfanas, consumo negativo, dias invalidos, incidencias invalidas y periodos invalidos en cuarentena.
- PQR: tipar columnas y estandarizar fechas a ISO, soportando `dd/MM/yyyy` y `yyyy-MM-dd`.


Las tablas derivadas se reconstruyen de forma determinista desde Bronze.


## Gold y Quality Gate


Gold contiene un registro por cliente activo. Uso y PQR se agregan por separado antes del join para evitar fan-out. Se mantiene:


- `churn_risk` con la ventana reproducible de cuatro meses basada en la fecha maxima disponible en PQR Silver.
- `upsell_flag` cuando el consumo promedio supera `1.80 × capacidad_gb`.
- Bloqueo de la publicacion si Gold esta vacio, hay identificadores nulos o duplicados, `churn_risk` fuera de dominio, consumo negativo o PQR abiertos mayores que PQR totales.
- Conservacion de la ultima publicacion valida cuando el Quality Gate falla.


## Orquestacion


Se recomienda un Databricks Job con tareas independientes y dependencias explicitas:


1. Bronze Cliente, Producto, Uso y PQR en paralelo.
2. Reconocimiento/validacion de entrada.
3. Silver Cliente y Producto en paralelo.
4. Silver Uso y Silver PQR segun sus dependencias.
5. Gold y Quality Gate.
6. Analitica P5.


Configuracion recomendada:


- Una sola ejecucion concurrente del producto.
- Dos reintentos controlados por tarea.
- Alertas ante fallo del Job o incumplimiento del SLA.
- Schedule diario, con disponibilidad antes de las 08:00 hora Colombia.
- El bootstrap (`00_bootstrap_unity_catalog.py`) se ejecuta una vez por ambiente, no como tarea diaria.


## Evolucion productiva


Como evolucion futura, la version de prueba usa `overwrite` porque los CSV representan el dataset completo y se necesita reproducibilidad. En produccion se evolucionara a:


1. Landing inmutable por lote y fecha.
2. Registro de control con archivo, hash, tamaño, estado y conteos.
3. Ignorar archivos ya procesados mediante hash.
4. `MERGE` por llave de negocio: `id_cliente`, `id_producto`, `id_uso` e `id_caso`.
5. Watermark por lote o periodo.
6. Recalcular Gold completo o solo clientes impactados segun volumen.


## Optimizacion


Para `fact_uso_servicio_silver` a gran escala:


- Liquid Clustering por `periodo`, `id_cliente` e `id_producto`, si esta disponible en el workspace.
- Como alternativa, particionar por `periodo` y aplicar `OPTIMIZE`/`ZORDER` segun el patron real de consulta.
- No particionar por `id_cliente` por su alta cardinalidad.
- Evaluar auto compaction, optimized writes y tamaño de archivos.
- Definir la retencion de `VACUUM` segun auditoria y time travel.


Estas optimizaciones no son necesarias para las 50 filas de la prueba.


## Versionamiento y cambios


Los notebooks, Jobs, parametros y dependencias deben versionarse en Git. La modificacion de una ruta se hace en la definicion del Job o en Databricks Asset Bundles, no manualmente en cada consumidor. El cambio se valida con `databricks bundle validate`, pruebas y Pull Request antes del despliegue.


## Seguridad y gobierno


Unity Catalog administra catalogo, permisos, linaje y auditoria. Gold no publica `documento` ni `nombre_completo`; Mercadeo Digital consume una vista autorizada con minimo privilegio.