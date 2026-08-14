{{
    config(
        materialized='view',
        tags=['staging', 'loans']
    )
}}

-- =============================================================================
-- Staging model: stg_loans
-- =============================================================================
-- Traduce silver_loans al lenguaje del negocio antes de modelar marts.
--
-- Convenciones aplicadas:
--   - Renombro columnas tecnicas a nombres claros de negocio
--     (ej. loan_amnt -> loan_amount, emp_title -> employer_title).
--   - Casteos explicitos donde hace falta.
--   - Derivo fico_score_avg como el promedio del rango FICO
--     (Lending Club expone rango, la mayoria de los analisis usan el promedio).
--   - NO agrego business logic ni agregaciones: eso va en marts.
--
-- Materializacion:
--   view por dos razones:
--     1. Los datos ya viven fisicamente en silver (Delta en S3), no tiene sentido
--        duplicar storage en gold para una simple renombracion.
--     2. Iterar es mas rapido: cambio nombres y no reproceso data.
-- =============================================================================

with source as (

    select * from {{ source('silver', 'silver_loans') }}

),

renamed as (

    select
        -- Identificadores
        id                                              as loan_id,

        -- Terminos del prestamo
        loan_amnt                                       as loan_amount,
        funded_amnt                                     as funded_amount,
        term_months                                     as term_in_months,
        int_rate                                        as interest_rate,
        installment                                     as monthly_installment,
        grade                                           as loan_grade,
        sub_grade                                       as loan_sub_grade,

        -- Info del deudor
        emp_title                                       as employer_title,
        emp_length_years                                as employment_years,
        home_ownership                                  as home_ownership_status,
        annual_inc_clean                                as annual_income,
        is_income_outlier                               as annual_income_was_outlier,
        verification_status                             as income_verification_status,
        dti                                             as debt_to_income_ratio,
        application_type                                as application_type,

        -- Origen y proposito
        purpose                                         as loan_purpose,
        title                                           as loan_title,
        addr_state                                      as borrower_state,
        zip_code                                        as borrower_zip_code,

        -- Fechas
        issue_d_parsed                                  as origination_date,
        last_pymnt_d_parsed                             as last_payment_date,
        earliest_cr_line_parsed                         as first_credit_line_date,

        -- Estado y performance
        loan_status_normalized                          as loan_status,
        delinquency_bucket                              as delinquency_bucket,
        pymnt_plan                                      as has_payment_plan,
        out_prncp                                       as outstanding_principal,
        total_pymnt                                     as total_paid,
        total_rec_prncp                                 as principal_paid,
        total_rec_int                                   as interest_paid,
        total_rec_late_fee                              as late_fees_paid,
        recoveries                                      as recovered_amount,

        -- FICO
        fico_range_low                                  as fico_score_low,
        fico_range_high                                 as fico_score_high,
        (fico_range_low + fico_range_high) / 2.0        as fico_score_avg,

        -- Utilizacion revolvente
        revol_util                                      as revolving_utilization,

        -- Metadata para trazabilidad end-to-end
        _ingested_at                                    as ingested_at,
        _batch_id                                       as bronze_batch_id,
        _silver_batch_id                                as silver_batch_id,

        -- Particion
        issue_year                                      as origination_year

    from source

)

select * from renamed