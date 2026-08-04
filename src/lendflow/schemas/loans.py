"""
Schema del bronze layer para los prestamos de Lending Club.

Elegi un enfoque "tipado pero tolerante": los numeros y textos van con su tipo
correcto, pero las fechas las dejo como string y las parseo en silver. La razon
es que las fechas del dataset vienen en formatos raros ("Dec-2018") y si intento
castear en bronze me arriesgo a que la ingesta falle por una fila mala. Prefiero
que bronze sea resiliente y silver haga el trabajo sucio.

Tambien agrego tres columnas de metadata (_ingested_at, _source_file, _batch_id)
que no vienen del source, las inyecta el pipeline. Sirven para trazabilidad:
saber cuando y de donde vino cada fila si aparece un bug en produccion.
"""
##importamos las clases que vamos a necesitar
from pyspark.sql.types import (
    DoubleType,  # tipo decimal (números con coma)
    StringType,  # tipo texto
    StructField,  # cada columna individual dentro del schema
    StructType,  # la "caja" que contiene todo el schema
    TimestampType,  # tipo fecha con hora
)

# Columnas que vienen directamente del CSV de Lending Club.
# Este es el schema que usa el reader al leer el archivo raw.
LOANS_RAW_SCHEMA = StructType([
    # Identificador. Lo dejo como string aunque parezca numero.
    StructField("id", StringType(), nullable=False),

    # Monto solicitado y monto realmente financiado.
    StructField("loan_amnt", DoubleType(), nullable=True),
    StructField("funded_amnt", DoubleType(), nullable=True),

    # term viene como "36 months" o "60 months". Lo parseo a int en silver.
    StructField("term", StringType(), nullable=True),

    # Tasa de interes y cuota mensual.
    StructField("int_rate", DoubleType(), nullable=True),
    StructField("installment", DoubleType(), nullable=True),

    # grade va de A (menor riesgo) a G (mayor riesgo).
    # sub_grade es la subdivision fina: A1, A2, A3... hasta G5.
    StructField("grade", StringType(), nullable=True),
    StructField("sub_grade", StringType(), nullable=True),

    # Titulo del puesto que reporto el deudor. Texto libre, muy sucio.
    StructField("emp_title", StringType(), nullable=True),

    # Antiguedad laboral. Viene como "< 1 year", "10+ years", etc.
    StructField("emp_length", StringType(), nullable=True),

    # Situacion habitacional: RENT, OWN, MORTGAGE, OTHER.
    StructField("home_ownership", StringType(), nullable=True),

    # Ingreso anual declarado.
    StructField("annual_inc", DoubleType(), nullable=True),

    # Si Lending Club verifico o no el ingreso declarado.
    StructField("verification_status", StringType(), nullable=True),

    # Debt to income ratio.
    StructField("dti", DoubleType(), nullable=True),

    # Individual o Joint (con codeudor).
    StructField("application_type", StringType(), nullable=True),

    # Proposito del prestamo y titulo libre.
    StructField("purpose", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),

    # zip_code enmascarado ("310xx") y estado.
    StructField("zip_code", StringType(), nullable=True),
    StructField("addr_state", StringType(), nullable=True),

    # Fechas: formato "Dec-2018". Se parsean en silver.
    StructField("issue_d", StringType(), nullable=True),
    StructField("last_pymnt_d", StringType(), nullable=True),
    StructField("earliest_cr_line", StringType(), nullable=True),

    # loan_status: Current, Fully Paid, Charged Off, Late, etc.
    StructField("loan_status", StringType(), nullable=True),

    # Indica si el prestamo esta en plan de pago especial.
    StructField("pymnt_plan", StringType(), nullable=True),

    # Saldo pendiente y desglose de pagos.
    StructField("out_prncp", DoubleType(), nullable=True),
    StructField("total_pymnt", DoubleType(), nullable=True),
    StructField("total_rec_prncp", DoubleType(), nullable=True),
    StructField("total_rec_int", DoubleType(), nullable=True),
    StructField("total_rec_late_fee", DoubleType(), nullable=True),

    # Monto recuperado post charge-off.
    StructField("recoveries", DoubleType(), nullable=True),

    # FICO score al momento de originar (rango).
    StructField("fico_range_low", DoubleType(), nullable=True),
    StructField("fico_range_high", DoubleType(), nullable=True),

    # Utilizacion de credito revolvente. Numero entre 0 y ~100.
    # Originalmente pensaba que venia con "%" pero al validar contra el CSV
    # aparecen sin el simbolo, asi que va como Double.
    StructField("revol_util", DoubleType(), nullable=True),
])


# Schema completo del bronze final: raw + metadata de ingesta.
# Estas 3 columnas las agrega el pipeline, no vienen del source.
LOANS_BRONZE_SCHEMA = StructType(
    LOANS_RAW_SCHEMA.fields + [
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
        StructField("_batch_id", StringType(), nullable=False),
    ]
)