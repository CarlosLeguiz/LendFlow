"""
Utilidades para el dashboard de LendFlow.

Encapsula la conexion a Athena y ejecucion de queries.
Usa cache de Streamlit para evitar re-ejecutar queries pesadas
en cada interaccion del usuario.
"""

import os
import pandas as pd
import streamlit as st
from pyathena import connect

# =============================================================================
# Configuracion de conexion a Athena
# =============================================================================
# Los valores vienen de las credenciales de AWS CLI (~/.aws/credentials)
# o de variables de entorno si se corre en cloud.
# =============================================================================

AWS_REGION = "us-east-2"
ATHENA_STAGING_DIR = "s3://lendflow-athena-results-851563823943/staging/"
ATHENA_SCHEMA = "lendflow_dev"


@st.cache_resource
def get_athena_connection():
    """
    Crea la conexion a Athena. Cacheada como resource singleton para
    reutilizarla entre queries y evitar establecer sesiones nuevas
    innecesariamente.
    """
    return connect(
        s3_staging_dir=ATHENA_STAGING_DIR,
        region_name=AWS_REGION,
        schema_name=ATHENA_SCHEMA,
    )


@st.cache_data(ttl=3600)
def run_query(query: str) -> pd.DataFrame:
    """
    Ejecuta un query en Athena y devuelve el resultado como DataFrame.

    Cache TTL de 1 hora: Streamlit guarda el resultado en memoria y
    lo reusa si se llama con el mismo query. Se refresca cada hora
    por si hay data nueva en el gold layer.

    Uso en la app:
        df = run_query("SELECT * FROM fct_originations LIMIT 10")
    """
    conn = get_athena_connection()
    return pd.read_sql(query, conn)


def format_currency(value: float) -> str:
    """Formatea un numero como USD sin decimales: 1234567 -> '$1,234,567'."""
    return f"${value:,.0f}"


def format_number(value: float) -> str:
    """Formatea un numero con separador de miles: 1234567 -> '1,234,567'."""
    return f"{value:,.0f}"


def format_pct(value: float, decimals: int = 1) -> str:
    """Formatea un numero como porcentaje: 12.5 -> '12.5%'."""
    return f"{value:.{decimals}f}%"