"""
LendFlow Pipeline DAG.

Orquesta el pipeline end-to-end de LendFlow:
    bronze_ingest → silver_transform → vacuum_delta → sync_to_s3 → dbt_run → dbt_test

Todas las tasks corren dentro del container de Airflow, usando volume mounts
al codigo del proyecto (montados en /opt/lendflow).

Autor: Carlos Leguizamon
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# =============================================================================
# Argumentos por defecto para todas las tasks
# =============================================================================
default_args = {
    "owner": "carlos_leguizamon",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# =============================================================================
# Definicion del DAG
# =============================================================================
with DAG(
    dag_id="lendflow_pipeline",
    default_args=default_args,
    description="Pipeline end-to-end de LendFlow: bronze -> silver -> S3 -> dbt",
    schedule=None,  # solo ejecucion manual por ahora
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lendflow", "data-pipeline", "manual"],
) as dag:

    # =========================================================================
    # Task 0: Placeholder de inicio
    # =========================================================================
    start = EmptyOperator(task_id="start")

    # =========================================================================
    # Task 1: Bronze - Ingesta CSV a Parquet
    # =========================================================================
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=(
            "cd /opt/lendflow && "
            "python -m src.lendflow.bronze.ingest_loans"
        ),
        doc_md="""
        ### Bronze Ingest

        Convierte el CSV crudo de Lending Club a Parquet particionado
        por `issue_year`. Aplica schema explicito por seleccion de columnas
        (evita el bug de mapeo por posicion en Spark).

        Output: `data/bronze/loans/` (2.26M filas).
        """,
    )

    # =========================================================================
    # Task 2: Silver - Transformacion con Delta Lake
    # =========================================================================
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=(
            "cd /opt/lendflow && "
            "python -m src.lendflow.silver.clean_loans"
        ),
        doc_md="""
        ### Silver Transform

        Aplica parseo de fechas, normalizacion de loan_status, cap de outliers
        de ingreso, y agrega `delinquency_bucket`. Escribe en Delta Lake.

        Output: `data/silver/loans/` (formato Delta).
        """,
    )

    # =========================================================================
    # Task 3: Vacuum Delta - Limpiar archivos historicos
    # =========================================================================
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

        Elimina archivos historicos del Delta Lake para evitar que Athena
        (que lee el path como Parquet) vea archivos duplicados de versiones
        anteriores. Preserva unicamente los archivos activos.
        """,
    )

    # =========================================================================
    # Task 4: Sync a S3
    # =========================================================================
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
        Flag `--delete` remueve archivos remotos que ya no existen localmente,
        garantizando consistencia entre local y cloud.
        """,
    )

    # =========================================================================
    # Task 5: dbt Run - Materializar gold layer
    # =========================================================================
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/lendflow/lendflow_dbt && "
            "dbt run --profiles-dir /opt/lendflow/lendflow_dbt"
        ),
        doc_md="""
        ### dbt Run

        Ejecuta todos los modelos dbt del gold layer:
        stg_loans, dim_date, dim_borrower, dim_loan_grade, fct_originations.
        Materializa las tablas en Athena via Glue Catalog.
        """,
    )

    # =========================================================================
    # Task 6: dbt Test - Validar calidad
    # =========================================================================
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/lendflow/lendflow_dbt && "
            "dbt test --profiles-dir /opt/lendflow/lendflow_dbt"
        ),
        doc_md="""
        ### dbt Test

        Corre todos los tests de calidad definidos en los YAMLs de dbt.
        Valida integridad referencial, unicidad, valores permitidos y
        reglas de negocio (invariantes como funded_amount <= loan_amount).
        """,
    )

    # =========================================================================
    # Task 7: Placeholder de fin
    # =========================================================================
    end = EmptyOperator(task_id="end")

    # =========================================================================
    # Dependencias del DAG
    # =========================================================================
    (
        start
        >> bronze_ingest
        >> silver_transform
        >> vacuum_delta
        >> sync_to_s3
        >> dbt_run
        >> dbt_test
        >> end
    )