{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

-- =============================================================================
-- Dimension: dim_borrower
-- =============================================================================
-- Grano: un deudor por prestamo (borrower_key = loan_id).
--
-- Nota importante sobre el grano:
--   El dataset de Lending Club no tiene un identificador unico de deudor.
--   Un mismo deudor puede tener multiples prestamos y no hay forma de
--   identificarlo. Por eso adoptamos la convencion "un borrower por loan":
--   cada prestamo tiene su snapshot del deudor al momento de originar.
--
--   Consecuencia: no podemos hacer analisis de "cliente recurrente" con
--   este dataset. En un caso real con borrower_id verdadero, esta dim
--   seria SCD Type 2 y agruparia todos los prestamos del mismo deudor.
--
-- Uso principal:
--   - Segmentacion en dashboards (por FICO bucket, income bucket, region).
--   - Enriquecer facts con atributos derivados sin recalcularlos en cada query.
-- =============================================================================

with stg as (

    select * from {{ ref('stg_loans') }}

),

enriched as (

    select
        -- =====================================================================
        -- Surrogate key
        -- =====================================================================
        -- Un borrower por prestamo (convencion documentada arriba).
        loan_id                                             as borrower_key,

        -- =====================================================================
        -- FICO score
        -- =====================================================================
        fico_score_avg                                      as fico_score,

        case
            when fico_score_avg < 580 then 'poor'
            when fico_score_avg < 670 then 'fair'
            when fico_score_avg < 740 then 'good'
            when fico_score_avg < 800 then 'very_good'
            when fico_score_avg >= 800 then 'exceptional'
        end                                                 as fico_bucket,

        -- =====================================================================
        -- Ingreso anual
        -- =====================================================================
        annual_income,

        case
            when annual_income is null then 'unknown'
            when annual_income < 30000 then 'low_under_30k'
            when annual_income < 60000 then 'mid_30k_60k'
            when annual_income < 100000 then 'upper_mid_60k_100k'
            when annual_income < 150000 then 'high_100k_150k'
            when annual_income >= 150000 then 'very_high_over_150k'
        end                                                 as income_bucket,

        -- =====================================================================
        -- Empleo
        -- =====================================================================
        employment_years,

        case
            when employment_years is null then 'unknown'
            when employment_years = 0 then 'less_than_1_year'
            when employment_years <= 3 then 'junior_1_3_years'
            when employment_years <= 7 then 'mid_4_7_years'
            when employment_years >= 8 then 'senior_8_plus_years'
        end                                                 as employment_bucket,

        -- =====================================================================
        -- Debt to income
        -- =====================================================================
        debt_to_income_ratio,

        case
            when debt_to_income_ratio is null then 'unknown'
            when debt_to_income_ratio < 10 then 'low_under_10'
            when debt_to_income_ratio < 20 then 'moderate_10_20'
            when debt_to_income_ratio < 30 then 'high_20_30'
            when debt_to_income_ratio >= 30 then 'very_high_over_30'
        end                                                 as dti_bucket,

        -- =====================================================================
        -- Atributos categoricos (sin derivar, se copian tal cual)
        -- =====================================================================
        home_ownership_status,
        income_verification_status,
        borrower_state,

        -- =====================================================================
        -- Region (derivada del estado)
        -- =====================================================================
        -- Agrupacion estandar de US Census Bureau: 4 regiones.
        case
            when borrower_state in ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') then 'Northeast'
            when borrower_state in ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') then 'Midwest'
            when borrower_state in ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') then 'South'
            when borrower_state in ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') then 'West'
            else 'Unknown'
        end                                                 as borrower_region,

        -- =====================================================================
        -- Metadata de linaje
        -- =====================================================================
        cast(current_timestamp as timestamp(3))             as dbt_updated_at,
        '{{ invocation_id }}'                               as dbt_invocation_id

    from stg

)

select * from enriched