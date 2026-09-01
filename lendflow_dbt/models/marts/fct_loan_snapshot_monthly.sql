{{
    config(
        materialized='table',
        tags=['marts', 'facts'],
        partitioned_by=['snapshot_year'],
        enabled=false
    )
}}

-- =============================================================================
-- Fact: fct_loan_snapshot_monthly
-- =============================================================================
-- GRANO: un prestamo x un mes (snapshot mensual).
--
-- Genera snapshots sinteticos mensuales de cada prestamo desde su fecha de
-- originacion hasta el mes de su ultimo pago (o hasta la fecha actual si
-- sigue vigente).
--
-- LIMITACION IMPORTANTE - LEER ANTES DE USAR:
--
--   El dataset de Lending Club NO tiene snapshots historicos reales de
--   loan_status. Solo sabemos el status ACTUAL del prestamo.
--
--   Este modelo aproxima usando la siguiente logica simple:
--     - Todos los meses intermedios (desde origination_date hasta antes
--       del ultimo pago) se marcan como 'Current'.
--     - El ultimo mes (correspondiente a last_payment_date) usa el
--       status final real del prestamo.
--
--   Esta aproximacion es aceptable para vintage curves y volumenes,
--   pero NO refleja transiciones reales de mora. Los roll rates
--   calculados sobre este fact seran artificialmente extremos
--   (jump directo de 'current' a 'charged_off' sin pasar por
--   los buckets intermedios).
--
--   EN UN CASO REAL:
--   Este fact se construiria desde una de estas fuentes:
--     1. Snapshots mensuales capturados por un job programado
--        (Airflow corriendo cada 1ro de mes contra la base transaccional).
--     2. Change Data Capture (CDC) via Debezium o AWS DMS que captura
--        cada cambio de estado del prestamo.
--     3. Event sourcing donde cada accion (pago, mora, refinanciacion)
--        emite un evento y el estado se deriva de la agregacion.
--
-- MODELO DESHABILITADO (enabled=false):
--   Este modelo no se materializa automaticamente en dbt run por su
--   alto costo (~70M filas, 5-10 min de ejecucion en Athena).
--   Para activarlo, cambiar enabled=true en el config o correr
--   explicitamente con dbt run --select fct_loan_snapshot_monthly.
--
-- Uso principal (cuando este activo):
--   - Vintage curves (delinquency rate por cohort x meses de vida).
--   - Portfolio at Risk por mes.
--   - Roll rates (con la limitacion arriba mencionada).
-- =============================================================================

with loans as (

    select
        loan_id,
        origination_date,
        last_payment_date,
        loan_status,
        delinquency_bucket,
        outstanding_principal,
        loan_amount,
        funded_amount,
        term_in_months
    from {{ ref('stg_loans') }}

),

-- Explota cada prestamo en N filas segun cantidad de meses de vida.
-- Join con dim_date filtrando solo el primer dia de cada mes.
loan_months as (

    select
        l.loan_id,
        l.origination_date,
        l.last_payment_date,
        l.loan_status                                       as final_loan_status,
        l.delinquency_bucket                                as final_delinquency_bucket,
        l.outstanding_principal,
        l.loan_amount,
        l.funded_amount,
        l.term_in_months,

        date_trunc('month', d.full_date)                    as snapshot_month,

        -- Meses desde originacion (edad del prestamo). Clave para vintage curves.
        date_diff('month', l.origination_date, d.full_date) as months_on_book

    from loans l
    inner join {{ ref('dim_date') }} d
        on d.full_date >= date_trunc('month', l.origination_date)
        and d.full_date <= coalesce(l.last_payment_date, current_date)
        and day_of_month(d.full_date) = 1

),

-- Aplica la aproximacion documentada arriba:
--   - Meses intermedios: 'Current'.
--   - Ultimo mes: status real.
enriched as (

    select
        loan_id,

        -- Surrogate key para join con dim_date
        cast(date_format(snapshot_month, '%Y%m%d') as integer) as snapshot_date_key,

        snapshot_month,
        year(snapshot_month)                                 as snapshot_year,
        month(snapshot_month)                                as snapshot_month_num,
        months_on_book,

        -- Cohort de originacion (para vintage analysis)
        cast(date_format(origination_date, '%Y%m%d') as integer) as origination_date_key,
        date_format(origination_date, '%Y-%m')                   as origination_cohort,

        -- Status estimado del prestamo en este snapshot (ver limitacion)
        case
            when snapshot_month = date_trunc('month', coalesce(last_payment_date, current_date))
                then final_loan_status
            else 'Current'
        end                                                  as loan_status_at_snapshot,

        case
            when snapshot_month = date_trunc('month', coalesce(last_payment_date, current_date))
                then final_delinquency_bucket
            else 'current'
        end                                                  as delinquency_bucket_at_snapshot,

        -- Metricas base (congeladas al momento de originar por simplicidad)
        loan_amount,
        funded_amount,
        outstanding_principal,
        term_in_months,

        -- Metadata de linaje
        cast(current_timestamp as timestamp(3))              as dbt_updated_at,
        '{{ invocation_id }}'                                as dbt_invocation_id

    from loan_months

)

select * from enriched