"""
Factory de SparkSession para los jobs del proyecto.

La idea es que ningun job cree su propia SparkSession a mano. Todos pasan por
esta funcion y asi las configs quedan en un solo lugar.

"""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """
    Devuelve una SparkSession lista para usar en local.

    El app_name es lo que aparece en el Spark UI, asi que conviene ponerle
    algo descriptivo (por ejemplo "bronze_loans" en vez de "test1") para
    saber que corrida estas mirando cuando abris el UI.
    """
    return (
        SparkSession.builder
        .appName(app_name)

        # --- Recursos ---
        # 4GB al driver alcanza para el CSV de Lending Club sin OOMs.
        .config("spark.driver.memory", "4g")

        # --- Comportamiento por defecto ---
        # UTC en toda la sesion. En fintech mezclar timezones es la fuente
        # numero uno de bugs silenciosos, y despues es un dolor debuggearlos.
        .config("spark.sql.session.timeZone", "UTC")

        # --- Performance: Adaptive Query Execution ---
        # AQE es el mejor upgrade de Spark 3.x. Ajusta el plan de ejecucion
        # en runtime segun lo que ve en los datos: coalesce de particiones,
        # cambio de join strategy, manejo de skew. Siempre encendido.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        # --- Performance: particiones y joins ---
        # Por default son 200 particiones de shuffle, cosa exagerada para local.
        # En cluster grande el AQE lo auto-ajusta, aca lo forzamos bajo.
        .config("spark.sql.shuffle.partitions", "8")

        # Joins con tablas < 10MB se broadcastean automaticamente en vez de shuffle.
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")

        # --- Serializacion ---
        # Kryo es mas rapido que la serializacion Java por default.
        # Es un no-brainer, se prende siempre.
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # --- Compresion ---
        # Snappy es un buen default: buena compresion, muy rapido para leer.
        # Lo dejo explicito para que quede claro que estoy usando.
        .config("spark.sql.parquet.compression.codec", "snappy")

        .getOrCreate()
    )