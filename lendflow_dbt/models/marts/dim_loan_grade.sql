{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

-- =============================================================================
-- Dimension: dim_loan_grade
-- =============================================================================
-- Grano: un sub-grado unico (A1, A2, ..., G5). 35 filas totales.
--
-- Enriquece cada sub_grade con:
--   - Risk tier (agrupacion senior: prime / near_prime / subprime / deep_subprime).
--   - Estadisticas historicas de tasa (min, max, avg).
--   - Volumen historico originado (loan_count, total_amount).
--
-- Uso principal:
--   - Segmentacion en dashboards por risk_tier.
--   - Analisis de concentracion de portfolio por grado.
--   - Comparacion de tasas actuales vs historicas por grado.
--
-- Fuente: fct_originations (agregado a nivel sub_grade).
-- =============================================================================

with fct as (

    select * from {{ ref('fct_originations') }}

),

aggregated as (

    select
        -- Surrogate key: sub_grade es unico por naturaleza (A1..G5).
        loan_sub_grade                                      as grade_key,
        loan_grade,
        loan_sub_grade,

        -- Estadisticas de tasa por sub_grade
        round(avg(interest_rate), 2)                        as avg_interest_rate,
        round(min(interest_rate), 2)                        as min_interest_rate,
        round(max(interest_rate), 2)                        as max_interest_rate,

        -- Volumen historico
        count(*)                                            as loan_count,
        round(sum(loan_amount), 2)                          as total_originated_amount,
        round(avg(loan_amount), 2)                          as avg_loan_amount

    from fct
    group by loan_grade, loan_sub_grade

),

enriched as (

    select
        grade_key,
        loan_grade,
        loan_sub_grade,

        -- Risk tier: agrupacion estandar de credit risk en fintech.
        case
            when loan_grade = 'A' then 'prime'
            when loan_grade in ('B', 'C') then 'near_prime'
            when loan_grade in ('D', 'E') then 'subprime'
            when loan_grade in ('F', 'G') then 'deep_subprime'
        end                                                 as risk_tier,

        avg_interest_rate,
        min_interest_rate,
        max_interest_rate,
        loan_count,
        total_originated_amount,
        avg_loan_amount,

        -- Metadata de linaje
        cast(current_timestamp as timestamp(3))             as dbt_updated_at,
        '{{ invocation_id }}'                               as dbt_invocation_id

    from aggregated

)

select * from enriched
order by loan_sub_grade