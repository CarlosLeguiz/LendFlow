"""
LendFlow Dashboard: storytelling analitico del portfolio de Lending Club.

Consume el gold layer en Athena (fct_originations, dim_borrower, dim_loan_grade)
y presenta 5 vistas ejecutivas:
  1. KPIs generales del portfolio.
  2. Evolucion mensual de originaciones.
  3. Mix por risk tier.
  4. Top 10 estados por volumen.
  5. Rentabilidad esperada por cohort anual.

Ejecutar localmente:
    streamlit run dashboard/app.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils import run_query, format_currency, format_number, format_pct


# =============================================================================
# Paleta de colores
# =============================================================================
PRIMARY = "#9C8B85"      # beige oscuro / topo
SECONDARY = "#D9B8AE"    # rosa palo
TERTIARY = "#EED7C5"     # nude claro
ACCENT = "#F7E3D0"       # melocoton suave
NEUTRAL = "#B5A8A0"      # gris calido

# Mapa ordinal para risk tier (mejor -> peor)
RISK_TIER_COLORS = {
    "prime": PRIMARY,
    "near_prime": SECONDARY,
    "subprime": TERTIARY,
    "deep_subprime": ACCENT,
}

# Layout base para todos los graficos de Plotly (uniforme)
PLOTLY_LAYOUT = dict(
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(family="sans-serif", color="#3E2E2A", size=13),
    xaxis=dict(gridcolor="#F0EAE5", linecolor=NEUTRAL, zerolinecolor="#F0EAE5"),
    yaxis=dict(gridcolor="#F0EAE5", linecolor=NEUTRAL, zerolinecolor="#F0EAE5"),
    margin=dict(l=20, r=20, t=40, b=40),
)


# =============================================================================
# Config de pagina
# =============================================================================
st.set_page_config(
    page_title="LendFlow | Portfolio Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =============================================================================
# Header
# =============================================================================
st.title("LendFlow: Portfolio Analytics")
st.markdown(
    """
    Storytelling analitico del portfolio historico de **Lending Club** (2007-2018).
    Data procesada end-to-end con **PySpark + Delta Lake + dbt + Athena**.
    """
)
st.divider()


# =============================================================================
# Seccion 1: KPIs generales
# =============================================================================
st.header("Snapshot del portfolio")

kpis_query = """
SELECT
    COUNT(*) AS total_loans,
    SUM(funded_amount) AS total_funded,
    AVG(funded_amount) AS avg_ticket,
    AVG(interest_rate) AS avg_rate
FROM fct_originations
"""

kpis = run_query(kpis_query).iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total prestamos", format_number(kpis["total_loans"]))
col2.metric("Capital financiado", format_currency(kpis["total_funded"]))
col3.metric("Ticket promedio", format_currency(kpis["avg_ticket"]))
col4.metric("Tasa promedio", format_pct(kpis["avg_rate"], 2))

st.markdown(
    """
    > **Contexto:** Lending Club opero como plataforma peer-to-peer entre 2007 y 2020.
    > Este dataset publico cubre los primeros 12 años, con **2.26 millones de prestamos**
    > por un total superior a **USD 30 mil millones**.
    """
)

st.divider()


# =============================================================================
# Seccion 2: Evolucion mensual
# =============================================================================
st.header("Evolucion mensual de originaciones")

monthly_query = """
SELECT
    d.year_month AS year_month_label,
    COUNT(*) AS loan_count,
    SUM(f.funded_amount) AS total_funded
FROM fct_originations f
JOIN dim_date d ON f.origination_date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month
"""

monthly = run_query(monthly_query)

fig_monthly = go.Figure()
fig_monthly.add_trace(
    go.Scatter(
        x=monthly["year_month_label"],
        y=monthly["total_funded"],
        mode="lines",
        name="Capital financiado",
        line=dict(color=PRIMARY, width=2),
        fill="tozeroy",
        fillcolor="rgba(156, 139, 133, 0.15)",  # PRIMARY con transparencia
    )
)
fig_monthly.update_layout(
    **PLOTLY_LAYOUT,
    height=400,
    xaxis_title="Mes",
    yaxis_title="USD",
    hovermode="x unified",
    showlegend=False,
)
st.plotly_chart(fig_monthly, use_container_width=True)

st.markdown(
    """
    > **Insight:** El volumen crece explosivamente entre 2013 y 2015 (peak),
    > luego se estabiliza y cae hacia 2018 cuando Lending Club empieza a
    > perder market share frente a competidores como SoFi.
    """
)

st.divider()


# =============================================================================
# Seccion 3: Mix por risk tier
# =============================================================================
st.header("Distribucion del portfolio por nivel de riesgo")

risk_query = """
SELECT
    g.risk_tier,
    COUNT(*) AS loan_count,
    SUM(f.funded_amount) AS total_funded,
    AVG(f.interest_rate) AS avg_rate
FROM fct_originations f
JOIN dim_loan_grade g ON f.loan_sub_grade = g.grade_key
GROUP BY g.risk_tier
ORDER BY
    CASE g.risk_tier
        WHEN 'prime' THEN 1
        WHEN 'near_prime' THEN 2
        WHEN 'subprime' THEN 3
        WHEN 'deep_subprime' THEN 4
    END
"""

risk = run_query(risk_query)

col1, col2 = st.columns([1, 1])

with col1:
    fig_pie = px.pie(
        risk,
        values="loan_count",
        names="risk_tier",
        color="risk_tier",
        color_discrete_map=RISK_TIER_COLORS,
        hole=0.5,
    )
    fig_pie.update_traces(textposition="outside", textinfo="percent+label")
    fig_pie.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="sans-serif", color="#3E2E2A", size=13),
        margin=dict(l=20, r=20, t=40, b=40),
        height=400,
        showlegend=False,
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.markdown("#### Tasa promedio por risk tier")
    for _, row in risk.iterrows():
        st.metric(
            row["risk_tier"].replace("_", " ").title(),
            format_pct(row["avg_rate"], 2),
            help=f"{format_number(row['loan_count'])} prestamos",
        )

st.markdown(
    """
    > **Insight:** El portfolio esta dominado por **near_prime** (grades B y C),
    > que representa el segmento mas rentable para Lending Club: tasas altas
    > pero con mora manejable. Prime (grade A) es minoritario porque compite
    > con bancos tradicionales.
    """
)

st.divider()


# =============================================================================
# Seccion 4: Top estados
# =============================================================================
st.header("Top 10 estados por volumen")

states_query = """
SELECT
    borrower_state,
    COUNT(*) AS loan_count,
    SUM(funded_amount) AS total_funded
FROM fct_originations
GROUP BY borrower_state
ORDER BY loan_count DESC
LIMIT 10
"""

states = run_query(states_query)

fig_states = px.bar(
    states.sort_values("loan_count"),
    x="loan_count",
    y="borrower_state",
    orientation="h",
    text="loan_count",
)
fig_states.update_traces(
    texttemplate="%{text:,.0f}",
    textposition="outside",
    marker_color=SECONDARY,
)
fig_states.update_layout(
    **PLOTLY_LAYOUT,
    height=400,
    xaxis_title="Prestamos originados",
    yaxis_title="Estado",
    showlegend=False,
)
st.plotly_chart(fig_states, use_container_width=True)

st.markdown(
    """
    > **Insight:** California, Texas y Nueva York concentran el mayor volumen,
    > alineado con su tamaño poblacional. Sin embargo, la penetracion per capita
    > es mas alta en estados como Nevada o Florida, sugiriendo mayor apetito por
    > credito no bancario en esas regiones.
    """
)

st.divider()


# =============================================================================
# Seccion 5: Rentabilidad esperada por cohort
# =============================================================================
st.header("Rentabilidad esperada por cohort de originacion")

cohort_query = """
SELECT
    origination_year,
    SUM(funded_amount) AS total_funded,
    SUM(expected_interest) AS total_expected_interest,
    100.0 * SUM(expected_interest) / SUM(funded_amount) AS expected_yield_pct
FROM fct_originations
GROUP BY origination_year
ORDER BY origination_year
"""

cohort = run_query(cohort_query)

fig_cohort = go.Figure()
fig_cohort.add_trace(
    go.Bar(
        x=cohort["origination_year"],
        y=cohort["total_funded"],
        name="Capital financiado",
        marker_color=PRIMARY,
        yaxis="y",
    )
)
fig_cohort.add_trace(
    go.Scatter(
        x=cohort["origination_year"],
        y=cohort["expected_yield_pct"],
        name="Yield esperado (%)",
        mode="lines+markers",
        line=dict(color=SECONDARY, width=3),
        marker=dict(size=10),
        yaxis="y2",
    )
)
fig_cohort.update_layout(
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(family="sans-serif", color="#3E2E2A", size=13),
    margin=dict(l=20, r=20, t=40, b=40),
    height=450,
    xaxis=dict(title="Anio de originacion", gridcolor="#F0EAE5", linecolor=NEUTRAL),
    yaxis=dict(title="Capital (USD)", side="left", gridcolor="#F0EAE5", linecolor=NEUTRAL),
    yaxis2=dict(
        title="Yield esperado (%)",
        overlaying="y",
        side="right",
        gridcolor="#F0EAE5",
    ),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center"),
    hovermode="x unified",
)
st.plotly_chart(fig_cohort, use_container_width=True)

st.markdown(
    """
    > **Insight:** El yield esperado sube consistentemente año a año, indicando que
    > Lending Club fue subiendo tasas para mantener spreads en un contexto de
    > mayor competencia. Sin embargo, el yield ESPERADO no considera charge-offs:
    > la rentabilidad realizada suele ser 3-5 puntos porcentuales menor.
    """
)

st.divider()


# =============================================================================
# Footer
# =============================================================================
st.markdown(
    """
    ---
    **Sobre este dashboard**
    Construido con **Streamlit + Plotly**, consumiendo el gold layer de LendFlow
    directamente desde Athena. Todas las metricas se calculan on the fly con SQL
    contra las tablas `fct_originations`, `dim_date`, `dim_borrower`, `dim_loan_grade`.

    [Repositorio en GitHub](https://github.com/CarlosLeguiz/LendFlow) ·
    [Documentacion dbt](https://carlosleguiz.github.io/LendFlow/)
    """
)