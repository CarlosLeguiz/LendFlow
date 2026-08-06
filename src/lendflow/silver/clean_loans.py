"""
Job de silver layer para loans.

Toma el bronze (Parquet particionado por anio) y lo transforma en silver
(Delta Lake con tipos correctos, columnas normalizadas, y outliers manejados).

Transformaciones que aplico:
- Fechas: parseo string "Dec-2018" a DateType.
- term: extraigo el numero de meses de "36 months" -> 36.
- emp_length: mapeo "10+ years" -> 10, "< 1 year" -> 0, etc.
- loan_status: normalizo los 2 estados legacy a los estandar.
- delinquency_bucket: creo columna derivada para roll rates.
- annual_inc: capeo outliers >10M y marco con flag.
- Agrego metadata de procesamiento silver.

Ejecutar:
    uv run python -m lendflow.silver.clean_loans
"""

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lendflow.utils.spark_session import get_spark

# Paths del pipeline
BRONZE_PATH = "data/bronze/loans"
SILVER_PATH = "data/silver/loans"

# Threshold para capear outliers de annual_inc
ANNUAL_INC_MAX = 10_000_000

# Funciones de transformacion (puras, testeables)
def parse_dates(df: DataFrame) -> DataFrame:
    """
    Convierte las 3 columnas de fecha de string "MMM-yyyy" a DateType.

    Las fechas vienen como "Dec-2018", "Jan-2005", etc. Uso to_date con el
    formato explicito. Si algo viene mal formateado, to_date devuelve null
    (comportamiento default de Spark 3.x), lo que esta bien para bronze
    porque quiero preservar la fila con la fecha en null en vez de fallar.
    """
    date_cols = ["issue_d", "last_pymnt_d", "earliest_cr_line"]
    for col in date_cols:
        df = df.withColumn(f"{col}_parsed", F.to_date(col, "MMM-yyyy"))
    return df

def parse_term(df: DataFrame) -> DataFrame:
    """
    Convierte term de " 36 months" a integer 36.

    El valor viene con un espacio adelante y el texto "months" al final.
    Extraigo solo los digitos con regexp_extract.
    """
    return df.withColumn(
        "term_months",
        F.regexp_extract(F.col("term"), r"(\d+)", 1).cast("int"),
    )

def parse_emp_length(df: DataFrame) -> DataFrame:
    """
    Convierte emp_length de "10+ years" / "< 1 year" a integer.

    Estrategia: extraigo los digitos de cada valor. Los casos especiales:
    - "< 1 year" -> 0 (menos de un anio de antiguedad)
    - "10+ years" -> 10 (10 o mas anios)
    - null se mantiene null.

    La regex \\d+ captura el numero, cast a int lo convierte.
    Para "< 1 year" el regex captura "1", pero lo tratamos como 0 con when.
    """
    return df.withColumn(
        "emp_length_years",
        F.when(
            F.col("emp_length") == "< 1 year", F.lit(0)
        ).when(
            F.col("emp_length").isNotNull(),
            F.regexp_extract(F.col("emp_length"), r"(\d+)", 1).cast("int"),
        ).otherwise(F.lit(None)),
    )

def normalize_loan_status(df: DataFrame) -> DataFrame:
    """
    Normaliza los 2 estados legacy de loan_status a los estandar.

    Lending Club tiene 2 prestamos "Does not meet the credit policy..."
    que son legacy. Los mapeo a los estados estandar (Fully Paid,
    Charged Off). Los otros 7 estados quedan iguales.
    """
    return df.withColumn(
        "loan_status_normalized",
        F.when(
            F.col("loan_status") == "Does not meet the credit policy. Status:Fully Paid",
            F.lit("Fully Paid"),
        ).when(
            F.col("loan_status") == "Does not meet the credit policy. Status:Charged Off",
            F.lit("Charged Off"),
        ).otherwise(F.col("loan_status")),
    )

def add_delinquency_bucket(df: DataFrame) -> DataFrame:
    """
    Agrega la columna delinquency_bucket, la mas importante del proyecto.

    Alimenta vintage curves, roll rates y portfolio at risk. Los buckets
    son los estandar de credit risk:
    - current: al dia (incluye Fully Paid e In Grace Period).
    - late_early: 16-30 dias de mora.
    - late_late: 31-120 dias de mora.
    - default: estado intermedio antes de charge-off (raro, ~40 casos).
    - charged_off: default declarado, el banco escribio la perdida.
    """
    return df.withColumn(
        "delinquency_bucket",
        F.when(
            F.col("loan_status_normalized").isin("Current", "Fully Paid", "In Grace Period"),
            F.lit("current"),
        ).when(
            F.col("loan_status_normalized") == "Late (16-30 days)",
            F.lit("late_early"),
        ).when(
            F.col("loan_status_normalized") == "Late (31-120 days)",
            F.lit("late_late"),
        ).when(
            F.col("loan_status_normalized") == "Default",
            F.lit("default"),
        ).when(
            F.col("loan_status_normalized") == "Charged Off",
            F.lit("charged_off"),
        ).otherwise(F.lit("unknown")),
    )

def cap_income_outliers(df: DataFrame) -> DataFrame:
    """
    Capea outliers absurdos de annual_inc.

    El max original era USD 110 millones, imposible (errores de tipeo con
    ceros de mas). Aplico regla: si annual_inc > ANNUAL_INC_MAX (10M),
    lo dejo en null y marco la fila con is_income_outlier=true para
    trazabilidad. Asi los analisis descartan estos casos sin perder la
    referencia de que existieron.
    """
    return (
        df
        .withColumn(
            "is_income_outlier",
            F.col("annual_inc") > ANNUAL_INC_MAX,
        )
        .withColumn(
            "annual_inc_clean",
            F.when(
                F.col("annual_inc") > ANNUAL_INC_MAX,
                F.lit(None),
            ).otherwise(F.col("annual_inc")),
        )
    )

def add_silver_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """
    Agrega columnas de trazabilidad del procesamiento silver.

    _silver_processed_at: cuando se corrio este job (UTC).
    _silver_batch_id: identificador unico de esta corrida silver.

    Se complementan con las de bronze (_ingested_at, _source_file, _batch_id)
    para poder trackear una fila desde el CSV original hasta silver.
    """
    processed_at = datetime.now(timezone.utc)
    return (
        df
        .withColumn("_silver_processed_at", F.lit(processed_at).cast("timestamp"))
        .withColumn("_silver_batch_id", F.lit(batch_id))
    )

# Job orquestador
class SilverLoansJob:
    """
    Orquesta la transformacion bronze -> silver.

    La clase compone las funciones puras de transformacion en el orden correcto.
    Cada paso es una linea, facil de leer. Si manana necesito debuggear una
    transformacion, se donde ir y como testearla en aislamiento.
    """

    def __init__(self, spark: SparkSession, source_path: str, target_path: str):
        self.spark = spark
        self.source_path = source_path
        self.target_path = target_path
        self.batch_id = str(uuid4())

    def read_bronze(self) -> DataFrame:
        """Lee el bronze en Parquet."""
        return self.spark.read.parquet(self.source_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Aplica todas las transformaciones en orden."""
        return (
            df
            .transform(parse_dates)
            .transform(parse_term)
            .transform(parse_emp_length)
            .transform(normalize_loan_status)
            .transform(add_delinquency_bucket)
            .transform(cap_income_outliers)
            .transform(lambda d: add_silver_metadata(d, self.batch_id))
        )

    def write_silver(self, df: DataFrame) -> None:
        """Escribe el resultado como Delta particionado por issue_year."""
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("issue_year")
            .save(self.target_path)
        )

    def run(self) -> None:
        """Ejecuta el pipeline completo con logging."""
        print(f"Iniciando silver loans")
        print(f"  Source: {self.source_path}")
        print(f"  Destino: {self.target_path}")
        print(f"  Batch ID: {self.batch_id}")

        df_bronze = self.read_bronze()
        df_silver = self.transform(df_bronze)

        row_count = df_silver.count()
        print(f"Filas a escribir: {row_count:,}")

        self.write_silver(df_silver)
        print(f"Silver loans completado en {self.target_path}")


# Entry point
def main() -> None:
    """Entry point del job silver."""
    spark = get_spark("silver_loans")
    try:
        job = SilverLoansJob(spark, BRONZE_PATH, SILVER_PATH)
        job.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()