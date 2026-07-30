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

LOANS_BRONZE_SCHEMA = StructType([
    # El id lo dejo como string aunque parezca numero. Nunca voy a sumarlo ni
    # promediarlo, y si algun dia Lending Club decide meterle letras no quiero
    # que se rompa todo.
    StructField("id", StringType(), nullable=False),

    # Monto solicitado y monto realmente financiado. Pueden diferir cuando el
    # inversor no cubre el total del pedido.
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
    # Lo dejo como string, lo mapeo a numero en silver.
    StructField("emp_length", StringType(), nullable=True),

    # Situacion habitacional: RENT, OWN, MORTGAGE, OTHER.
    StructField("home_ownership", StringType(), nullable=True),

    # Ingreso anual declarado.
    StructField("annual_inc", DoubleType(), nullable=True),

    # Si Lending Club verifico o no el ingreso declarado.
    StructField("verification_status", StringType(), nullable=True),

    # Debt to income ratio. Ratio deuda mensual / ingreso mensual.
    StructField("dti", DoubleType(), nullable=True),

    # Individual o Joint (con codeudor).
    StructField("application_type", StringType(), nullable=True),

    # Proposito del prestamo: debt_consolidation, credit_card, home_improvement, etc.
    # title es el texto libre que puso el deudor; suele ser mas sucio.
    StructField("purpose", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),

    # zip_code viene enmascarado por privacidad. Formato "310xx".
    # addr_state es el estado (CA, NY, TX, etc.).
    StructField("zip_code", StringType(), nullable=True),
    StructField("addr_state", StringType(), nullable=True),

    # Fecha de originacion del prestamo. Formato "Dec-2018".
    # La parseo en silver porque el formato es fragil.
    StructField("issue_d", StringType(), nullable=True),

    # Fecha del ultimo pago. Null si el prestamo esta charged off desde el arranque.
    StructField("last_pymnt_d", StringType(), nullable=True),

    # Fecha en que el deudor abrio su primer credito. Sirve para calcular
    # antiguedad crediticia en silver.
    StructField("earliest_cr_line", StringType(), nullable=True),

    # loan_status es la columna mas importante del proyecto.
    # Valores tipicos: Current, Fully Paid, Charged Off, Late (16-30 days),
    # Late (31-120 days), In Grace Period, Default, Issued.
    StructField("loan_status", StringType(), nullable=True),

    # Indica si el prestamo esta en algun plan de pago especial.
    StructField("pymnt_plan", StringType(), nullable=True),

    # Saldo pendiente de capital.
    StructField("out_prncp", DoubleType(), nullable=True),

    # Total pagado hasta la fecha (capital + interes + fees).
    StructField("total_pymnt", DoubleType(), nullable=True),

    # Desglose de lo pagado: capital, intereses, comisiones por mora.
    StructField("total_rec_prncp", DoubleType(), nullable=True),
    StructField("total_rec_int", DoubleType(), nullable=True),
    StructField("total_rec_late_fee", DoubleType(), nullable=True),

    # Monto recuperado post charge-off. Clave para calcular recovery rate.
    StructField("recoveries", DoubleType(), nullable=True),

    # FICO score al momento de originar. Viene como rango (low y high).
    # El score real cae en algun punto entre esos dos.
    StructField("fico_range_low", DoubleType(), nullable=True),
    StructField("fico_range_high", DoubleType(), nullable=True),

    # Utilizacion de credito revolvente. Viene con el "%" al final ("32.5%"),
    # por eso queda como string; se limpia y castea en silver.
    StructField("revol_util", StringType(), nullable=True),

    # Metadata de ingesta.
    StructField("_ingested_at", TimestampType(), nullable=False),
    StructField("_source_file", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
])