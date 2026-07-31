"""
Job de ingesta bronze para loans de Lending Club.

Lee el CSV raw, aplica el schema explicito, agrega metadata de ingesta
y escribe el resultado como Parquet particionado por anio de originacion.

El anio se saca de issue_d, que viene como "Dec-2018". Como en bronze
issue_d es string, extraigo el anio con un substring simple sin castear
la fecha completa. En silver hacemos el parse completo.

Ejecutar:
    uv run python -m lendflow.bronze.ingest_loans
"""
#importamos los modulos a usar 
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lendflow.schemas.loans import LOANS_RAW_SCHEMA
from lendflow.utils.spark_session import get_spark

# Paths del pipeline. Por ahora hardcoded, mas adelante van a config/yaml.
RAW_PATH = "data/raw/accepted_2007_to_2018Q4.csv.gz"
BRONZE_PATH = "data/bronze/loans"


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    """
    Lee el CSV raw aplicando el schema explicito.

    Uso mode="PERMISSIVE" que es el default: si una fila esta mal formateada,
    Spark la carga con nulls en las columnas problematicas en vez de fallar.
    Es lo que queremos en bronze: ser tolerantes a suciedad y limpiar en silver.
    """
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        # El CSV tiene comillas con comas adentro (texto libre). Spark las maneja bien pero conviene ser explicito.
        .option("quote", '"')
        .option("escape", '"')
        .schema(LOANS_RAW_SCHEMA)
        .csv(path)
    )


def add_ingest_metadata(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Agrega las 3 columnas de metadata al DataFrame.

    _ingested_at: cuando corrio este job (UTC).
    _source_file: de que archivo salio (util cuando ingerimos varios).
    _batch_id: identificador unico de esta corrida (UUID).
    """
    ingested_at = datetime.now(timezone.utc)
    return (
        df
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def add_partition_column(df: DataFrame) -> DataFrame:
    """
    Extrae el anio de issue_d para usarlo como columna de particion.

    issue_d viene como "Dec-2018", asi que agarro los ultimos 4 caracteres.
    Si el valor es null o mal formateado, queda como "unknown" y va a esa
    particion. En silver despues limpiamos.
    """
    return df.withColumn(
        "issue_year",
        F.when(
            F.col("issue_d").isNotNull() & (F.length("issue_d") >= 4),
            F.substring("issue_d", -4, 4),
        ).otherwise(F.lit("unknown")),
    )


def write_bronze(df: DataFrame, output_path: str) -> None:
    """
    Escribe el DataFrame como Parquet particionado por issue_year.

    Uso mode="overwrite" para desarrollo. En produccion esto seria "append"
    o un MERGE si estuvieramos usando Delta Lake.
    """
    (
        df.write
        .mode("overwrite")
        .partitionBy("issue_year")
        .parquet(output_path)
    )


def main() -> None:
    """Orquesta el pipeline completo de ingesta bronze."""
    spark = get_spark("bronze_loans")

    # Genero un batch_id unico para esta corrida.
    batch_id = str(uuid4())
    source_file = Path(RAW_PATH).name

    print(f"Iniciando ingesta bronze")
    print(f"  Source: {RAW_PATH}")
    print(f"  Destino: {BRONZE_PATH}")
    print(f"  Batch ID: {batch_id}")

    # Pipeline: leer -> agregar metadata -> agregar particion -> escribir.
    df_raw = read_raw_csv(spark, RAW_PATH)
    df_with_metadata = add_ingest_metadata(df_raw, source_file, batch_id)
    df_final = add_partition_column(df_with_metadata)

    # Antes de escribir, cuento filas para tener un log claro.
    row_count = df_final.count()
    print(f"Filas a escribir: {row_count:,}")

    write_bronze(df_final, BRONZE_PATH)

    print(f"Ingesta bronze completada en {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()