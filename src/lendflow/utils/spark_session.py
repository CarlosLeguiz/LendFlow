"""
Factory de SparkSession para los jobs del proyecto.

La idea es que ningun job cree su propia SparkSession a mano. Todos pasan por
esta funcion y asi las configs quedan en un solo lugar.

"""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """
    Devuelve una SparkSession lista para usar en local con Delta Lake activado.

    El app_name es lo que aparece en el Spark UI, asi que conviene ponerle
    algo descriptivo (por ejemplo "bronze_loans" en vez de "test1") para
    saber que corrida estas mirando cuando abris el UI.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)

        # --- Recursos ---
        .config("spark.driver.memory", "4g")

        # --- Comportamiento por defecto ---
        .config("spark.sql.session.timeZone", "UTC")

        # --- Performance: Adaptive Query Execution ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        # --- Performance: particiones y joins ---
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")

        # --- Serializacion ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # --- Compresion ---
        .config("spark.sql.parquet.compression.codec", "snappy")

        # --- Delta Lake ---
        # Sin estas dos configs Spark no reconoce el formato "delta".
        # La primera activa las extensiones SQL de Delta (MERGE, OPTIMIZE, etc).
        # La segunda hace que las tablas Delta se comporten como el catalog default.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    # Este helper descarga el JAR de Delta la primera vez y lo cachea.
    return configure_spark_with_delta_pip(builder).getOrCreate()