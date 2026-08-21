# LendFlow

Pipeline end-to-end de analytics para credit & collections en fintech.
Procesa el dataset publico de Lending Club (2.26M prestamos, 2007-2018)
desde ingesta cruda hasta modelo dimensional listo para dashboards.

Proyecto personal para demostrar practicas senior de data engineering:
arquitectura lakehouse sobre AWS, modelado dimensional con dbt, IaC con
Terraform, y decisiones documentadas con evidencia.

---

## Arquitectura

CSV crudo (Lending Club)
│
▼
┌───────────────────────────┐
│ BRONZE (PySpark) │
│ Parquet particionado │
│ 2,260,668 filas │
└──────────────┬────────────┘
│
▼
┌───────────────────────────┐
│ SILVER (PySpark + Delta) │
│ Cleaning + parseo tipos │
│ Delinquency buckets │
│ Outliers capeados │
└──────────────┬────────────┘
│ (sync a S3)
▼
┌───────────────────────────┐
│ S3 + Glue Catalog │
│ Delta Lake catalogado │
└──────────────┬────────────┘
│
▼
┌───────────────────────────┐
│ GOLD (dbt + Athena) │
│ Star schema: │
│ stg_loans │
│ dim_date │
│ fct_originations │
└───────────────────────────┘


---

## Stack

| Capa | Tecnologia |
|---|---|
| Processing | PySpark 3.5, Delta Lake 3.2 |
| Storage | AWS S3 (Delta Lake) |
| Catalog | AWS Glue Data Catalog |
| Query engine | Amazon Athena |
| Modelado analitico | dbt-athena 1.11 |
| Infra as Code | Terraform |
| Runtime local | WSL2 Ubuntu, Python 3.11, uv |
| Testing calidad | dbt tests + dbt_utils |

---

## Estructura del repo

lendflow/
├── src/lendflow/ # Codigo PySpark
│ ├── bronze/ # Job de ingesta bronze
│ ├── silver/ # Job de transformacion silver
│ ├── schemas/ # StructTypes explicitos
│ └── utils/ # SparkSession factory
│
├── lendflow_dbt/ # Proyecto dbt para gold layer
│ ├── models/
│ │ ├── staging/ # stg_loans + sources + docs
│ │ └── marts/ # dim_date, fct_originations
│ ├── packages.yml
│ └── dbt_project.yml
│
├── terraform/ # IaC de AWS (S3, Glue, IAM)
│
├── notebooks/ # Analisis exploratorio (no productivo)
│ ├── 01_explore_lending_club.ipynb
│ └── 02_column_analysis.ipynb
│
├── docs/ # Documentacion de decisiones
│ └── data-selection.md
│
└── data/ # Data local (gitignored)
├── raw/ # CSV original de Kaggle
├── bronze/ # Parquet particionado
└── silver/ # Delta Lake


---

## Layers

### Bronze
Ingesta cruda del CSV a Parquet particionado por anio de originacion.
- Filtra 33 filas basura que Lending Club inserto como subtotales.
- Aplica schema explicito con seleccion por nombre (evita el bug clasico
  de Spark de mapear columnas por posicion cuando el CSV tiene mas
  columnas que el schema).
- Agrega metadata de trazabilidad (`_ingested_at`, `_source_file`, `_batch_id`).

### Silver
Bronze transformado y enriquecido, en formato Delta Lake.
- Parseo de fechas de string "MMM-yyyy" a DATE.
- Parseo de `term` ("36 months" → 36) y `emp_length` ("10+ years" → 10).
- Normalizacion de `loan_status`: mapeo de 2 estados legacy a los estandar.
- Nueva columna `delinquency_bucket` con los 5 buckets estandar de credit risk.
- Cap de outliers de `annual_inc` (>10M → null + flag `is_income_outlier`).
- Codigo modularizado en funciones puras testeables.

### Gold
Modelo dimensional estilo Kimball, materializado en Athena via dbt.

- **`stg_loans`** (view): renombra silver al lenguaje del negocio.
  Deriva `fico_score_avg`. Sin business logic (esa capa vive en marts).

- **`dim_date`** (table): calendario 2007-2020 con surrogate key
  `date_key` (INT YYYYMMDD). Atributos derivados (year, quarter, month,
  day_name, is_weekend, is_month_end).

- **`fct_originations`** (table particionada): fact principal.
  Grano = un prestamo al momento de originar. Incluye:
  - Surrogate keys para joins eficientes.
  - Denormalizacion controlada (grade, purpose, state) para evitar joins.
  - Metricas pre-calculadas (`total_expected_payment`, `expected_interest`,
    `funding_ratio`) para consumo instantaneo desde dashboards.
  - Snapshot del deudor congelado (SCD Type 1).
  - Metadata de linaje (`dbt_updated_at`, `dbt_invocation_id`).

---

## Data quality

Tests dbt sobre todas las capas del gold. Ejemplos:

- Invariantes de negocio: `funded_amount <= loan_amount`,
  `total_expected_payment > funded_amount`.
- Integridad referencial: `fct_originations.origination_date_key`
  existe en `dim_date.date_key`.
- Valores permitidos: `loan_grade in ['A', 'B', 'C', 'D', 'E', 'F', 'G']`,
  `delinquency_bucket in ['current', 'late_early', 'late_late', 'default', 'charged_off']`.
- Unicidad y no nulos en surrogate keys.

Hallazgos reales encontrados por los tests:
- 33 filas basura del CSV (subtotales de Lending Club).
- 4 nulls en flag booleano `is_income_outlier` (fixeado en silver).
- 88 casos (0.004%) con `total_expected_payment <= funded_amount`
  por errores de captura (installments absurdos como $4.93 para
  prestamos de $11k). Conservados con test en severity warn.

---

## Bugs resueltos

**Mapeo de columnas por posicion en Spark**
Spark aplica el schema por posicion, no por nombre, incluso con
`header=true`. Como el CSV tenia 151 columnas y el schema 33, cada
columna terminaba llenandose con el contenido de la columna que
estaba en esa misma posicion del CSV. La columna `issue_d` se
llenaba con el texto libre de `desc`, generando particiones
corruptas al extraer el anio.

Fix: leer el CSV sin schema forzado (todo como string) y
seleccionar las 33 columnas por NOMBRE, casteando al tipo correcto.

---

## Como correr localmente

Requiere: Python 3.11, Java 17, uv, AWS CLI configurado.

```bash
# Instalar dependencias
uv sync

# Descargar dataset de Kaggle (Lending Club) a data/raw/
# Nombre esperado: accepted_2007_to_2018Q4.csv.gz

# Bronze: CSV → Parquet particionado
uv run python -m lendflow.bronze.ingest_loans

# Silver: Bronze → Delta Lake
uv run python -m lendflow.silver.clean_loans

# Sync silver a S3 (requiere infra de terraform aplicada)
aws s3 sync data/silver/loans/ s3://lendflow-data-<account_id>/silver/loans/

# Gold: dbt sobre Athena
cd lendflow_dbt
uv run dbt deps
uv run dbt run
uv run dbt test
```

---

## Roadmap

- [x] Bronze layer (PySpark → Parquet)
- [x] Silver layer (PySpark → Delta Lake)
- [x] Infra AWS con Terraform (S3, Glue, IAM)
- [x] Gold layer con dbt-athena (stg_loans, dim_date, fct_originations)
- [ ] `fct_loan_snapshot_monthly` para vintage curves y roll rates
- [ ] `dim_borrower` y `dim_loan_grade`
- [ ] Dashboard con Evidence.dev (storytelling HTML)
- [ ] Orquestacion con Apache Airflow
- [ ] CI/CD con GitHub Actions
- [ ] Migrar silver a MERGE (upsert) en vez de overwrite
- [ ] Contracts enforcement en dbt

---

## Autor

Carlos Leguizamon Guillaumet
[github.com/CarlosLeguiz](https://github.com/CarlosLeguiz)
LinkedIn


