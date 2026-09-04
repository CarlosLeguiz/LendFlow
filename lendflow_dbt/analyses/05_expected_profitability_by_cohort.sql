-- =============================================================================
-- Analisis 05: Rentabilidad esperada por cohort de originacion
-- =============================================================================
-- Pregunta de negocio:
--   ¿Cuanta plata esperabamos generar en interes por cada anio de cohort?
--   ¿Los anios recientes tienen mejor rentabilidad esperada?
--
-- Metricas:
--   - total_funded: capital total desembolsado ese anio.
--   - total_expected_interest: interes total esperado a cobrar.
--   - expected_yield_pct: rendimiento esperado sobre capital.
--   - avg_term_months: plazo promedio ponderado.
--
-- Uso tipico:
--   Comparacion year over year. Identificar si Lending Club esta bajando
--   o subiendo la calidad de su portfolio en terminos de rentabilidad
--   esperada.
--
-- Nota: es rentabilidad ESPERADA al momento de originar, no realizada.
--       No considera charge-offs ni prepayments (para eso se necesita
--       fct_loan_snapshot_monthly, que esta deshabilitado).
-- =============================================================================

select
    origination_year,
    count(*)                                        as loan_count,
    round(sum(funded_amount), 2)                    as total_funded,
    round(sum(expected_interest), 2)                as total_expected_interest,
    round(
        100.0 * sum(expected_interest) / nullif(sum(funded_amount), 0),
        2
    )                                               as expected_yield_pct,
    round(avg(term_in_months), 1)                   as avg_term_months
from {{ ref('fct_originations') }}
group by origination_year
order by origination_year