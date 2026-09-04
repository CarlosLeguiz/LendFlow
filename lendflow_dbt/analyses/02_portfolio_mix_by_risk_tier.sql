-- =============================================================================
-- Analisis 02: Mix de portfolio por risk tier
-- =============================================================================
-- Pregunta de negocio:
--   ¿Como esta distribuido el portfolio de Lending Club entre los distintos
--   niveles de riesgo? ¿Que porcentaje es prime vs subprime?
--
-- Uso tipico:
--   Grafico de torta o barras apiladas. KPI ejecutivo.
-- =============================================================================

select
    g.risk_tier,
    count(*)                                        as loan_count,
    round(sum(f.funded_amount), 2)                  as total_funded,
    round(avg(f.interest_rate), 2)                  as avg_interest_rate,
    round(
        100.0 * count(*) / sum(count(*)) over (),
        2
    )                                               as pct_of_portfolio
from {{ ref('fct_originations') }} f
inner join {{ ref('dim_loan_grade') }} g
    on f.loan_sub_grade = g.grade_key
group by g.risk_tier
order by
    case g.risk_tier
        when 'prime' then 1
        when 'near_prime' then 2
        when 'subprime' then 3
        when 'deep_subprime' then 4
    end