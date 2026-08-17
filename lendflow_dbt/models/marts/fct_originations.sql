{{
    config(
        materialized='table',
        tags=['marts', 'facts'],
        partitioned_by=['origination_year']
    )
}}

-- =============================================================================
-- Fact: fct_originations
-- =============================================================================
-- Grano: un prestamo originado. Una fila = un prestamo en el momento en que
-- Lending Club lo emitio. Este fact congela el estado del prestamo y del
-- deudor al dia de la originacion, aunque cambien despues.
--
-- Uso principal:
--   - Metricas de originacion (volumen, mix por grado/estado/proposito).
--   - Base para vintage curves (joineado con dim_date por cohort).
--   - Analisis de rentabilidad esperada (interes proyectado).
--
-- Diseno senior aplicado:
--   1. Surrogate key origination_date_key (INT YYYYMMDD) para join con dim_date.
--   2. Denormalizacion controlada de atributos consultados frecuente
--      (grade, purpose, state) para evitar joins innecesarios.
--   3. Metricas pre-calculadas (total_expected_payment, expected_interest,
--      funding_ratio) para que el consumo del dashboard sea instantaneo.
--   4. Materializacion table por ahora: el dataset es historico y estatico.
--      Migrar a incremental con unique_key='loan_id' cuando llegue data nueva.
--
-- Particionado: por origination_year para acelerar queries por rango de anios
-- (que es lo mas comun en analisis de vintage y cohort).
-- =============================================================================

with stg as (

    select * from {{ ref('stg_loans') }}

),

enriched as (

    select
        -- =====================================================================
        -- Identificadores y llaves
        -- =====================================================================
        loan_id,

        -- Surrogate key para join con dim_date (formato YYYYMMDD).
        cast(date_format(origination_date, '%Y%m%d') as integer) as origination_date_key,

        -- =====================================================================
        -- Atributos denormalizados (los mas consultados)
        -- =====================================================================
        -- Se copian aca aunque vivan en dimensiones futuras, para evitar
        -- joins en el 90% de las queries analiticas.
        loan_grade,
        loan_sub_grade,
        loan_purpose,
        borrower_state,
        term_in_months,
        application_type,

        -- =====================================================================
        -- Metricas base (vienen del silver)
        -- =====================================================================
        loan_amount,
        funded_amount,
        interest_rate,
        monthly_installment,

        -- =====================================================================
        -- Metricas pre-calculadas (senior move: no dejarselo al dashboard)
        -- =====================================================================
        -- Total esperado a pagar durante toda la vida del prestamo.
        -- Es cuota mensual * cantidad de cuotas.
        round(monthly_installment * term_in_months, 2) as total_expected_payment,

        -- Interes esperado = total esperado - capital financiado.
        -- Es la ganancia bruta esperada de Lending Club por este prestamo.
        round(monthly_installment * term_in_months - funded_amount, 2) as expected_interest,

        -- Ratio de fondeo: cuanto del pedido efectivamente se financio.
        -- Casi siempre 1.0, pero cuando es < 1 indica que los inversores
        -- no cubrieron todo el pedido.
        round(funded_amount / nullif(loan_amount, 0), 4) as funding_ratio,

        -- =====================================================================
        -- Snapshot del deudor al momento de originar
        -- (SCD Type 1: congelado en este fact)
        -- =====================================================================
        fico_score_avg                 as borrower_fico_at_origination,
        annual_income                  as borrower_annual_income_at_origination,
        debt_to_income_ratio           as borrower_dti_at_origination,
        employment_years               as borrower_employment_years_at_origination,
        home_ownership_status          as borrower_home_ownership_at_origination,
        income_verification_status     as borrower_income_verified_at_origination,

        -- =====================================================================
        -- Metadata de linaje
        -- =====================================================================
        origination_date,
        cast(current_timestamp as timestamp(3))  as dbt_updated_at,
        '{{ invocation_id }}'          as dbt_invocation_id,

        -- Ultima columna del SELECT porque es la particion (requisito de Athena/Hive).
        origination_year

    from stg

)

select * from enriched