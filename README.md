# LendFlow

Pipeline end-to-end de analytics para credit & collections en fintech.
Procesa el dataset publico de Lending Club (2.26M prestamos, 2007-2018)
desde ingesta cruda hasta modelo dimensional listo para dashboards.

Proyecto personal para demostrar practicas senior de data engineering:
arquitectura lakehouse sobre AWS, modelado dimensional con dbt,
orquestacion con Airflow, IaC con Terraform, y decisiones documentadas
con evidencia.

---

## Arquitectura

```
                CSV crudo (Lending Club)
                        │
                        ▼
        ┌───────────────────────────────┐
        │  BRONZE (PySpark)             │
        │  Parquet particionado         │
        │  2,260,668 filas              │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │  SILVER (PySpark + Delta)     │
        │  Cleaning + parseo tipos      │
        │  Delinquency buckets          │
        │  Outliers capeados            │
        └───────────────┬───────────────┘
                        │  (sync a S3)
                        ▼
        ┌───────────────────────────────┐
        │  S3 + Glue Catalog            │
        │  Delta Lake catalogado        │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │  GOLD (dbt + Athena)          │
        │  Star schema:                 │
        │    - stg_loans                │
        │    - dim_date                 │
        │    - dim_borrower             │
        │    - dim_loan_grade           │
        │    - fct_originations         │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │  Consumo                      │
        │  - Dashboard Streamlit        │
        │  - dbt docs (GitHub Pages)    │
        │  - Analyses SQL               │
        └───────────────────────────────┘

Orquestacion end-to-end con Airflow (task groups por layer).
```

---

## Stack

| Capa | Tecnologia |
|---|---|
| Ingesta y processing | PySpark 3.5, Delta Lake 3.2 |
| Storage | AWS S3 (Delta Lake) |
| Catalog | AWS Glue Data Catalog |
| Query engine | Amazon Athena |
| Modelado analitico | dbt-athena 1.11 |
| Orquestacion | Apache Airflow 2.10 (Docker Compose) |
| Dashboard | Streamlit + Plotly |
| Infra as Code | Terraform |
| Runtime local | WSL2 Ubuntu, Python 3.11, uv |
| Testing calidad | dbt tests + dbt_utils |

---

## Documentacion online

- **dbt docs (lineage visual):** https://carlosleguiz.github.io/LendFlow/

---

## Estructura del repo

```
lendflow/
├── src/lendflow/           # Codigo PySpark
│   ├── bronze/             # Job de ingesta bronze
│   ├── silver/             # Job de transformacion silver
│   ├── schemas/            # StructTypes explicitos
│   └── utils/              # SparkSession factory
│
├── lendflow_dbt/           # Proyecto dbt para gold layer
│   ├── models/
│   │   ├── staging/        # stg_loans + sources + docs
│   │   └── marts/          # dim_date, dim_borrower, dim_loan_grade, fct_originations
│   ├── analyses/           # 5 queries de negocio (compiladas, no materializadas)
│   ├── packages.yml
│   └── dbt_project.yml
│
├── airflow/                # Orquestacion con Apache Airflow
│   ├── dags/               # DAG lendflow_pipeline
│   ├── docker-compose.yaml
│   └── README.md
│
├── dashboard/              # Dashboard Streamlit
│   ├── app.py              # UI con storytelling en 5 secciones
│   ├── utils.py            # Helpers de conexion a Athena
│   └── .streamlit/
│
├── terraform/              # IaC de AWS (S3, Glue, IAM)
│
├── notebooks/              # Analisis exploratorio
│   ├── 01_explore_lending_club.ipynb
│   └── 02_column_analysis.ipynb
│
├── docs/                   # Documentacion de decisiones
│   └── data-selection.md
│
├── .github/workflows/      # CI/CD (dbt docs deploy)
│
└── data/                   # Data local (gitignored)
    ├── raw/
    ├── bronze/
    └── silver/
```

---

## Layers

### Bronze
Ingesta cruda del CSV a Parquet particionado por anio de originacion.

- Filtra 33 filas basura que Lending Club inserto como subtotales.
- Aplica schema explicito con seleccion por nombre para evitar el bug
  clasico de Spark de mapear columnas por posicion cuando el CSV tiene
  mas columnas que el schema.
- Agrega metadata de trazabilidad (`_ingested_at`, `_source_file`,
  `_batch_id`).

### Silver
Bronze transformado y enriquecido, en formato Delta Lake.

- Parseo de fechas de string "MMM-yyyy" a DATE.
- Parseo de `term` ("36 months" a 36) y `emp_length` ("10+ years" a 10).
- Normalizacion de `loan_status`: mapeo de 2 estados legacy a los estandar.
- Nueva columna `delinquency_bucket` con los 5 buckets estandar de credit risk.
- Cap de outliers de `annual_inc` (>10M a null + flag `is_income_outlier`).
- Codigo modularizado en funciones puras testeables.

### Gold
Modelo dimensional estilo Kimball, materializado en Athena via dbt.

- **`stg_loans`** (view): renombra silver al lenguaje del negocio.
  Deriva `fico_score_avg`. Sin business logic (esa capa vive en marts).
- **`dim_date`** (table): calendario 2007-2020 con surrogate key
  `date_key` (INT YYYYMMDD). Atributos derivados (year, quarter, month,
  day_name, is_weekend, is_month_end).
- **`dim_borrower`** (table): un deudor por prestamo con buckets derivados
  (fico_bucket, income_bucket, employment_bucket, dti_bucket) y region
  geografica del US Census.
- **`dim_loan_grade`** (table): un sub-grade por fila (35 filas totales)
  con risk_tier derivado (prime, near_prime, subprime, deep_subprime) y
  estadisticas historicas.
- **`fct_originations`** (table particionada): fact principal.
  Grano = un prestamo al momento de originar. Incluye:
  - Surrogate keys para joins eficientes.
  - Denormalizacion controlada (grade, purpose, state) para evitar joins.
  - Metricas pre-calculadas (`total_expected_payment`, `expected_interest`,
    `funding_ratio`) para consumo instantaneo desde dashboards.
  - Snapshot del deudor congelado (SCD Type 1).
  - Metadata de linaje (`dbt_updated_at`, `dbt_invocation_id`).

---

## Orquestacion (Airflow)

DAG `lendflow_pipeline` orquesta el pipeline end-to-end con task groups
que reflejan la arquitectura medallion en la UI:

```
start >> bronze >> silver >> gold >> end
```

Cada task group contiene sus tasks internas:

- **bronze:** ingest_loans
- **silver:** transform_loans, vacuum_delta, sync_to_s3
- **gold:** dbt_run, dbt_test

Practicas senior aplicadas:

- Retries con exponential backoff.
- Timeouts por task (30 min).
- SLAs configurados (45 min).
- `max_active_runs=1` para evitar ejecuciones concurrentes.
- `doc_md` por task con inputs, outputs y contexto de negocio.

---

## Data quality

Tests dbt sobre todas las capas del gold. Ejemplos:

- Invariantes de negocio: `funded_amount <= loan_amount`,
  `total_expected_payment > funded_amount`.
- Integridad referencial: `fct_originations.origination_date_key`
  existe en `dim_date.date_key`.
- Valores permitidos: `loan_grade in ['A','B','C','D','E','F','G']`,
  `delinquency_bucket in [...]`.
- Unicidad y no nulos en surrogate keys.

Hallazgos reales encontrados por los tests:

- 33 filas basura del CSV (subtotales de Lending Club).
- 4 nulls en flag booleano `is_income_outlier` (fixeado en silver).
- 88 casos (0.004%) con `total_expected_payment <= funded_amount` por
  errores de captura (installments absurdos como $4.93 para prestamos
  de $11k). Conservados con test en severity warn.

---

## Bugs resueltos

**Mapeo de columnas por posicion en Spark**

Spark aplica el schema por posicion, no por nombre, incluso con
`header=true`. Como el CSV tenia 151 columnas y el schema 33, cada
columna terminaba llenandose con el contenido de la columna que
estaba en esa misma posicion del CSV. La columna `issue_d` se llenaba
con el texto libre de `desc`, generando particiones corruptas al
extraer el anio.

Fix: leer el CSV sin schema forzado (todo como string) y seleccionar
las 33 columnas por NOMBRE, casteando al tipo correcto.

**Archivos duplicados en S3 tras reprocessing**

Delta Lake versiona archivos: no borra fisicamente cuando cambia data,
solo marca en el `_delta_log/`. Al reprocesar silver y sincronizar a S3,
Athena (que lee el path como Parquet crudo) veia archivos viejos y
nuevos, duplicando filas.

Fix: ejecutar `VACUUM RETAIN 0 HOURS` sobre el Delta local antes del
sync a S3.

---

## Como correr localmente

Requiere: Python 3.11, Java 17, uv, AWS CLI configurado, Docker.

```bash
# Instalar dependencias
uv sync

# Descargar dataset de Kaggle (Lending Club) a data/raw/
# Nombre esperado: accepted_2007_to_2018Q4.csv.gz

# Provisionar infra AWS
cd terraform
terraform init
terraform apply

# Correr pipeline manualmente
uv run python -m lendflow.bronze.ingest_loans
uv run python -m lendflow.silver.clean_loans
aws s3 sync data/silver/loans/ s3://lendflow-data-<account_id>/silver/loans/

# Gold con dbt
cd lendflow_dbt
uv run dbt deps
uv run dbt run
uv run dbt test

# Levantar dashboard
uv run streamlit run dashboard/app.py

# Levantar Airflow
cd airflow
docker compose up -d
# UI en http://localhost:8082 (usuario: airflow / password: airflow)
```

---

## Roadmap

- [x] Bronze layer (PySpark, Parquet)
- [x] Silver layer (PySpark, Delta Lake)
- [x] Infra AWS con Terraform (S3, Glue, IAM)
- [x] Gold layer con dbt-athena (5 modelos productivos)
- [x] dbt docs en GitHub Pages
- [x] 5 queries de analisis en `analyses/`
- [x] Dashboard Streamlit
- [x] Orquestacion con Airflow (DAG con task groups)
- [ ] Cosmos para descomposicion de dbt tasks
- [ ] Deploy del dashboard a Streamlit Cloud
- [ ] CI/CD con tests dbt en cada PR
- [ ] Migrar silver a MERGE (upsert) en vez de overwrite
- [ ] `fct_loan_snapshot_monthly` con logica realista de transicion

---

## Autor

Carlos Leguizamon Guillaumet
[github.com/CarlosLeguiz](https://github.com/CarlosLeguiz)