# Prueba Tecnica - Claro Data Mesh

## Objetivo

Implementar el producto de datos `cliente_360_churn_upsell` para el dominio Postpago Residencial, consolidando informacion de clientes, productos, uso y PQR para activar campanas de retencion y upsell.

## Fuentes de datos

| Archivo | Descripcion |
|---|---|
| `dim_cliente.csv` | Maestro de clientes |
| `dim_producto.csv` | Catalogo de productos y planes |
| `fact_uso_servicio.csv` | Consumo mensual por cliente y producto |
| `fact_pqr.csv` | Peticiones, quejas y reclamos |

## Arquitectura

```text
Fuentes CSV
    |
    v
Bronze (L1 Raw) -> main.claro_postpago.l1_raw.*
    |
    v
Silver (L2 Curated) -> main.claro_postpago.l2_curated.*
    |
    v
Cuarentena -> main.claro_postpago.l2_curated.*_quarantine
    |
    v
Gold (L3 Certified) -> main.claro_postpago.l3_certified.cliente_360_churn_upsell
    |
    v
Consumidores -> Salesforce Data Cloud, Power BI y Mercadeo Digital
```

## Estructura del repositorio

```text
prueba-tecnica-claro-data-mesh/
├── src/
│   ├── ingestion/          # Ingesta a Bronze
│   ├── silver/             # Transformaciones Silver
│   ├── gold/               # Producto Gold
│   ├── quality/            # Controles de calidad
│   └── common/             # Utilidades compartidas
├── resources/              # Recursos estaticos
├── infrastructure/         # Infraestructura como codigo
├── tests/                  # Pruebas unitarias
├── docs/
│   ├── architecture/      # Diagramas
│   └── data_contract/      # Contratos de datos
├── .github/workflows/      # CI/CD
├── .gitignore
├── README.md
└── databricks.yml          # Databricks Asset Bundle
```

## Convenciones de nombres

### Tablas

| Capa | Patron | Ejemplo |
|---|---|---|
| Bronze | `main.claro_postpago.l1_raw.{tabla}_bronze` | `main.claro_postpago.l1_raw.dim_cliente_bronze` |
| Silver | `main.claro_postpago.l2_curated.{tabla}_silver` | `main.claro_postpago.l2_curated.dim_cliente_silver` |
| Cuarentena | `main.claro_postpago.l2_curated.{tabla}_quarantine` | `main.claro_postpago.l2_curated.fact_uso_servicio_quarantine` |
| Gold | `main.claro_postpago.l3_certified.{producto}` | `main.claro_postpago.l3_certified.cliente_360_churn_upsell` |

### Git

- `main`: rama estable.
- `dev`: desarrollo.
- `feature/{descripcion}`: nuevas funcionalidades.

Ejemplos de commits:

```bash
git commit -m "feat: add bronze ingestion"
git commit -m "fix: handle null estrato"
git commit -m "docs: add architecture diagram"
```

## Ejecucion del pipeline

### 1. Cargar CSV en Databricks

Usar Unity Catalog Volumes o la opcion Upload Data de la interfaz. No usar `/FileStore/tables/` en el entorno Serverless validado.

Ruta de trabajo prevista para archivos, sujeta a validacion de permisos:

```text
/Volumes/main/claro_postpago/<schema>/<volume>/
```

### 2. Ejecutar notebooks en orden

1. `src/ingestion/01_bronze_ingestion.ipynb`
2. `src/silver/02_silver_dimensions.ipynb`
3. `src/silver/03_silver_facts.ipynb`
4. `src/quality/04_quarantine_and_quality.ipynb`
5. `src/gold/05_gold_cliente_360.ipynb`
6. `src/quality/06_gold_quality_gate.ipynb`

### 3. Validar quality gate

- Verificar las metricas de calidad.
- Confirmar que Gold cumple las reglas de certificacion.
- Bloquear la publicacion si falla una regla critica.

### 4. Exportar entregables

- Notebooks: `.dbc` o enlace compartido.
- Diagrama: `.png` o `.pdf` desde draw.io.
- Respuestas teoricas: `.docx` o `.pdf`.
- Presentacion: `.pptx`.

## Entregables

| Codigo | Descripcion | Formato |
|---|---|---|
| E1 | Notebooks Bronze, Silver y Gold | `.dbc` o enlace |
| E2 | Scripts de calidad | `.py` o `.sql` |
| E3 | Diagrama de arquitectura | `.png` o `.pdf` |
| E4 | Respuestas teoricas T1-T7 | `.pdf` o `.docx` |
| E5 | Presentacion ejecutiva, maximo 10 laminas | `.pptx` |

## Data contract inicial

| Elemento | Propuesta |
|---|---|
| Producto | `cliente_360_churn_upsell` |
| Dominio | Postpago Residencial |
| Capa | L3 / Certified / Gold |
| Owner de negocio | Lider de Postpago Residencial |
| Owner tecnico | Data Engineer del dominio |
| Consumidores | Mercadeo Digital, Data Cloud y Power BI |
| Grano | Un registro por cliente activo |
| SLA | Actualizacion diaria, disponible antes de las 08:00 |
| Esquema minimo | `id_cliente`, `segmento`, `ciudad`, `producto_actual`, `consumo_promedio_gb`, `total_incidencias_red`, `total_pqr`, `pqr_abiertos`, `satisfaccion_promedio`, `churn_risk`, `upsell_flag` |
| Reglas de calidad | `id_cliente` no nulo y unico; `churn_risk` valido; satisfaccion entre 1 y 5; consumo no negativo |
| Seguridad | Acceso restringido y minimo privilegio |
| Trazabilidad | Fuente, fecha de ingesta y tablas de origen |
| Incumplimiento | No certificar Gold; generar alerta y preservar evidencia |

## Optimizacion P6

Para `fact_uso_servicio` a gran escala:

- Particionar por `periodo` si el volumen y los patrones de consulta lo justifican.
- Evaluar `OPTIMIZE` y clustering por `id_cliente` e `id_producto` segun el entorno.
- Evitar particionar por `id_cliente` debido a su alta cardinalidad.
- Aplicar `VACUUM` solo con una politica de retencion aprobada.

## Estado

El repositorio contiene la estructura inicial y el README. Los cuatro CSV aun deben recibirse para realizar el perfilamiento real y cerrar las reglas de transformacion.

## Proximos pasos

1. Recibir los cuatro CSV.
2. Perfilar estructura, volumen y calidad.
3. Aprobar decisiones de tratamiento.
4. Implementar Bronze, Silver, cuarentena y Gold.
5. Ejecutar quality gate y generar evidencias.
