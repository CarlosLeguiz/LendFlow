"""
LendFlow Pipeline DAG.

Orquesta el pipeline end-to-end de LendFlow con arquitectura medallion:

    ┌─────────┐   ┌─────────┐   ┌─────────┐
    │ BRONZE  │──▶│ SILVER  │──▶│  GOLD   │
    └─────────┘   └─────────┘   └─────────┘
        │             │             │
    Parquet       Delta+VACUUM   dbt models
                  +sync S3       +tests

Tasks agrupadas visualmente en 3 task groups (bronze, silver, gold) para
reflejar la arquitectura medallion en la UI de Airflow.

Autor: Carlos Leguizamon
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


# =============================================================================
# Argumentos por defecto para todas las tasks
# =============================================================================
# Los defaults se heredan por todas las tasks a menos que se overrideen
# explicitamente. En un pipeline real usariamos alertas por email y Slack,
# pero para el portfolio quedan las bases documentadas.
# =============================================================================
default_args = {
    "owner": "carlos_leguizamon",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                         # 2 reintentos automaticos
    "retry_delay": timedelta(minutes=2),  # espera entre reintentos
    "retry_exponential_backoff": True,    # cada retry duplica el delay
    "execution_timeout": timedelta(minutes=30),  # kill si tarda mas
    "sla": timedelta(minutes=45),         # alerta si tarda mas de esto
}


# =============================================================================
# Definicion del DAG
# =============================================================================
with DAG(
    dag_id="lendflow_pipeline",
    default_args=default_args,
    description="Pipeline end-to-end de LendFlow: bronze -> silver -> S3 -> dbt",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # evita 2 ejecuciones concurrentes del mismo DAG
    tags=["lendflow", "medallion", "data-pipeline"],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Task de inicio y fin (markers visuales)
    # =========================================================================
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================================================================
    # Task Group: BRONZE
    # =========================================================================
    with TaskGroup(group_id="bronze", tooltip="Ingesta de CSV a Parquet") as bronze:

        bronze_ingest = BashOperator(
            task_id="ingest_loans",
            bash_command=(
                "cd /opt/lendflow && "
                "python -m src.lendflow.bronze.ingest_loans"
            ),
            doc_md="""
            ### Bronze Ingest

            Convierte el CSV crudo de Lending Club a Parquet particionado
            por `issue_year`. Aplica schema explicito por seleccion de
            columnas para evitar el bug de mapeo por posicion en Spark.

            **Input:** `data/raw/accepted_2007_to_2018Q4.csv.gz`
            **Output:** `data/bronze/loans/` (2.26M filas, 12 particiones)
            """,
        )

    # =========================================================================
    # Task Group: SILVER
    # =========================================================================
    with TaskGroup(
        group_id="silver",
        tooltip="Transformacion Delta + housekeeping + sync S3",
    ) as silver:

        silver_transform = BashOperator(
            task_id="transform_loans",
            bash_command=(
                "cd /opt/lendflow && "
                "python -m src.lendflow.silver.clean_loans"
            ),
            doc_md="""
            ### Silver Transform

            Aplica cleaning y enriquecimiento sobre bronze:
            - Parseo de fechas (issue_d, last_pymnt_d, earliest_cr_line).
            - Parseo de term ("36 months" -> 36) y emp_length.
            - Normalizacion de loan_status a valores estandar.
            - Delinquency bucket derivado (current, late_early, late_late, default, charged_off).
            - Cap de outliers de annual_inc (>10M -> null + flag).

            **Output:** `data/silver/loans/` en formato Delta Lake.
            """,
        )

        vacuum_delta = BashOperator(
            task_id="vacuum_delta",
            bash_command=(
                "cd /opt/lendflow && "
                "python -c \"from pyspark.sql import SparkSession; "
                "from delta import configure_spark_with_delta_pip; "
                "spark = configure_spark_with_delta_pip("
                "SparkSession.builder.appName('vacuum')"
                ".config('spark.databricks.delta.retentionDurationCheck.enabled', 'false')"
                ").getOrCreate(); "
                "spark.sql('VACUUM delta.\\`/opt/lendflow/data/silver/loans\\` RETAIN 0 HOURS')\""
            ),
            doc_md="""
            ### Vacuum Delta

            Elimina archivos historicos del Delta Lake que ya no estan
            referenciados por la version actual. Necesario porque Athena
            lee el path como Parquet crudo y veria archivos duplicados
            si no limpiamos.
            """,
        )

        sync_to_s3 = BashOperator(
            task_id="sync_to_s3",
            bash_command=(
                "aws s3 sync /opt/lendflow/data/silver/loans/ "
                "s3://lendflow-data-851563823943/silver/loans/ "
                "--delete"
            ),
            doc_md="""
            ### Sync a S3

            Sube el silver Delta local al bucket S3 productivo.
            Flag `--delete` garantiza consistencia removiendo archivos
            remotos que ya no existen localmente.
            """,
        )

        silver_transform >> vacuum_delta >> sync_to_s3

    # =========================================================================
    # Task Group: GOLD
    # =========================================================================
    with TaskGroup(group_id="gold", tooltip="Modelado dimensional con dbt") as gold:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "cd /opt/lendflow/lendflow_dbt && "
                "dbt run --profiles-dir /opt/lendflow/lendflow_dbt"
            ),
            doc_md="""
            ### dbt Run

            Materializa el gold layer en Athena via Glue Catalog.

            **Modelos ejecutados:**
            - `stg_loans` (view)
            - `dim_date` (table)
            - `dim_borrower` (table)
            - `dim_loan_grade` (table)
            - `fct_originations` (table particionada)
            """,
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "cd /opt/lendflow/lendflow_dbt && "
                "dbt test --profiles-dir /opt/lendflow/lendflow_dbt"
            ),
            doc_md="""
            ### dbt Test

            Ejecuta todos los tests de calidad definidos en los YAMLs:
            unique, not_null, accepted_values, relationships, e invariantes
            de negocio (funded_amount <= loan_amount, etc).

            Falla el pipeline si algun test critical falla.
            """,
        )

        dbt_run >> dbt_test

    # =========================================================================
    # Dependencias del DAG (a nivel de task groups)
    # =========================================================================
    start >> bronze >> silver >> gold >> end