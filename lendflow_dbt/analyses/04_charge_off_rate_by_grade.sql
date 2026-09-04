-- =============================================================================
-- Analisis 04: Charge-off rate por grade
-- =============================================================================
-- Pregunta de negocio:
--   ¿Cuanto se pierde por default (charge-off) en cada grade de riesgo?
--   ¿La tasa de charge-off aumenta linealmente con el grade?
--
-- Metricas:
--   - loan_count: cantidad total de prestamos por grade.
--   - charged_off_count: cuantos terminaron en charge-off.
--   - charge_off_rate: porcentaje.
--   - avg_recovery: monto promedio recuperado post charge-off.
--
-- Uso tipico:
--   Validar el pricing por grade. Si D y E tienen similar charge-off rate
--   pero muy distintas tasas, hay oportunidad de optimizar.
--
-- Nota: usamos silver directamente porque necesitamos loan_status
--       (que no esta en fct_originations, que solo tiene datos "al originar").
-- =============================================================================

select
    grade                                           as loan_grade,
    count(*)                                        as loan_count,
    sum(case when loan_status_normalized = 'Charged Off' then 1 else 0 end)
                                                    as charged_off_count,
    round(
        100.0 * sum(case when loan_status_normalized = 'Charged Off' then 1 else 0 end)
             / count(*),
        2
    )                                               as charge_off_rate_pct,
    round(avg(case when loan_status_normalized = 'Charged Off'
                   then recoveries end), 2)         as avg_recovery
from {{ source('silver', 'silver_loans') }}
group by grade
order by grade
