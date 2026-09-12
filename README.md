# Cliente 360 — Riesgo de Churn y Oportunidad de Upsell

Prueba técnica para el cargo de Ingeniero(a) Data Mesh en Claro Colombia.

## Descripción

Pipeline batch que construye un producto de datos certificado (L3) para el dominio Postpago Residencial. Integra cuatro fuentes:

- `dim_cliente`: maestro de clientes.
- `dim_producto`: catálogo de productos.
- `fact_uso_servicio`: consumo mensual.
- `fact_pqr`: peticiones, quejas y reclamos.

El resultado es una tabla Gold con un registro por cliente activo, indicadores de `churn_risk` (Alto, Medio, Bajo) y `upsell_flag` (true/false).

## Arquitectura

```text
CSV → Bronze Delta → Silver → Gold + Quality Gate → Analitiva
```

- **Bronze**: preserva el dato original con metadatos de trazabilidad.
- **Silver**: normaliza, tipa, aplica reglas de calidad y enva errores a cuarentena.
- **Gold**: consolida, aplica reglas de negocio y ejecuta un Quality Gate antes de certificarse.
- **Analitica**: consulta de consumo para la pregunta P5.

## Estructura del repositorio

```text
.
├── databricks.yml
├── README.md
├── docs
│   ├── 08_estrategia_optimizacion_produccion.md
│   ├── architecture
│   │   └── cliente_360_architecture.drawio
│   └── data_contract
│       └── data_contract_cliente_360.md
├── infrastructure
│   └── 00_bootstrap_unity_catalog.py
├── src
│   └── notebooks
│       ├── 01_bronze_dim_cliente.py
│       ├── 02_bronze_dim_producto.py
│       ├── 03_bronze_fact_uso_servicio.py
│       ├── 04_bronze_fact_pqr.py
│       ├── 05_bronze_reconocimiento_calidad.py
│       ├── 10_silver_dim_cliente.py
│       ├── 11_silver_dim_producto.py
│       ├── 12_silver_fact_uso_servicio.py
│       ├── 13_silver_fact_pqr.py
│       ├── 20_gold_cliente_360.py
│       └── 21_analitica_p5.py
└── tests
    └── .gitkeep
```

## Requisitos

- Cuenta Databricks Free/Community activa.
- Unity Catalog habilitado.
- Acceso para crear catlogos, esquemas, tablas Delta y Volumes.

## Ejecucin manual

1. **Bootstrap (una vez por ambiente)**

   ```text
   infrastructure/00_bootstrap_unity_catalog.py
   ```

   Crea el catálogo `claro_postpago`, esquemas `l1_raw`, `l2_curated`, `l3_certified`, `ops` y el Volume para los CSV.

2. **Carga de CSV a Bronze**

   Ejecuta en cualquier orden:

   ```text
   src/notebooks/01_bronze_dim_cliente.py
   src/notebooks/02_bronze_dim_producto.py
   src/notebooks/03_bronze_fact_uso_servicio.py
   src/notebooks/04_bronze_fact_pqr.py
   ```

3. **Reconocimiento y calidad (opcional)**

   ```text
   src/notebooks/05_bronze_reconocimiento_calidad.py
   ```

   Perfila las cuatro tablas Bronze, identifica duplicados, nulos, referencias huéı´rfanas y errores de formato. No transforma Silver/Gold.

4. **Silver**

   ```text
   src/notebooks/10_silver_dim_cliente.py
   src/notebooks/11_silver_dim_producto.py
   src/notebooks/12_silver_fact_uso_servicio.py
   src/notebooks/13_silver_fact_pqr.py
   ```

   Aplica reglas de calidad, normalizacin y cuarentena.

5. **Gold**

   ```text
   src/notebooks/20_gold_cliente_360.py
   ```

   Construye el producto certificado con `churn_risk` y `upsell_flag`. Ejecuta el Quality Gate y bloquea la publicación si falla una regla crítica.

6. **Analitica P5**

   ```text
   src/notebooks/21_analitica_p5.py
   ```

   Consulta SQL que devuelve, por segmento y ciudad, el número de clientes en riesgo Alto y el promedio de satisfacción.

## Prximos pasos (produccin)

- Orquestacin con Databricks Jobs/Lakeflow.
- Control de lote y hash de archivo para evitar reprocesar insumos.
- MERGE incremental en lugar de overwrite completo.
- Liquid Clustering o particin por periodo + OPTIMIZE/ZORDER.
- Databricks Asset Bundles para desplegar entre dev, qa y prod.
- Mtricas, alertas y SLA de disponibilidad.

## Entregables

| Cdigo | Descripcin |
|---|---|
| E1 | Notebooks Bronze, Silver, Gold y analitica |
| E2 | Validaciones de calidad en Bronze y Silver |
| E3 | Diagrama de arquitectura en `docs/architecture/` |
| E4 | Respuestas tericas (documento aparte) |
| E5 | Presentacin ejecutiva (documento aparte) |
| E6 | Estrategia de optimizacin productiva (`08_estrategia_optimizacion_produccion.md`) |
| E7 | Data contract (`docs/data_contract/data_contract_cliente_360.md`) |

## Gobierno

- Unity Catalog administra catálogo, esquemas, permisos, linaje y auditora.
- Gold no publica `documento` ni `nombre_completo`.
- Mercadeo Digital consume una vista autorizada con mnimo privilegio.

## Licencia

Uso interno — Claro Colombia.