-- =============================================================================
-- Analisis 03: Top 10 estados por volumen de originacion
-- =============================================================================
-- Pregunta de negocio:
--   ¿En que estados de USA se concentra el mayor volumen de originaciones?
--   ¿Hay concentracion geografica o esta distribuido?
--
-- Metricas:
--   - loan_count: cantidad de prestamos.
--   - total_funded: monto financiado.
--   - pct_of_total: porcentaje sobre el total del pais.
--
-- Uso tipico:
--   Ranking en dashboard. Base para analisis de concentracion geografica.
-- =============================================================================

with state_totals as (
    select
        f.borrower_state,
        count(*)                                    as loan_count,
        round(sum(f.funded_amount), 2)              as total_funded
    from {{ ref('fct_originations') }} f
    group by f.borrower_state
),

national_total as (
    select sum(loan_count) as total_loans
    from state_totals
)

select
    s.borrower_state,
    s.loan_count,
    s.total_funded,
    round(100.0 * s.loan_count / n.total_loans, 2)  as pct_of_total
from state_totals s
cross join national_total n
order by s.loan_count desc
limit 10