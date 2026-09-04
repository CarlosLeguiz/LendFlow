-- =============================================================================
-- Analisis 01: Volumen de originacion mensual
-- =============================================================================
-- Pregunta de negocio:
--   ¿Como evoluciono el volumen de prestamos originados por Lending Club
--   a lo largo del tiempo (2007-2018)? ¿Cuando fueron los picos y valles?
--
-- Metricas:
--   - loan_count: cantidad de prestamos originados en el mes.
--   - total_funded: monto total efectivamente financiado (USD).
--   - avg_ticket: ticket promedio (USD).
--
-- Uso tipico:
--   Grafico de linea (evolucion mensual) o barras (por anio).
--   Base para vintage analysis.
-- =============================================================================

select
    d.year                                          as origination_year,
    d.month                                         as origination_month,
    d.year_month                                    as year_month_label,
    count(*)                                        as loan_count,
    round(sum(f.funded_amount), 2)                  as total_funded,
    round(avg(f.funded_amount), 2)                  as avg_ticket
from {{ ref('fct_originations') }} f
inner join {{ ref('dim_date') }} d
    on f.origination_date_key = d.date_key
group by
    d.year,
    d.month,
    d.year_month
order by
    d.year,
    d.month