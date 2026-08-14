{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

-- =============================================================================
-- Dimension: dim_date
-- =============================================================================
-- Calendario de fechas con atributos derivados. Cubre 2007 a 2020 para
-- englobar todo el rango de originaciones de Lending Club (2007-2018) mas
-- margen para pagos posteriores.
--
-- Grano: una fila por fecha (date_key = YYYYMMDD como int, para joins eficientes).
--
-- Materializacion:
--   table (no view) porque una dim_date se lee mucho y calcula poco:
--   conviene materializar los ~5000 registros y evitar recalcular en cada query.
--
-- Nota sobre generacion de fechas:
--   Athena no tiene generate_series nativo. Uso el macro de dbt_utils que
--   ya viene instalado y funciona en Athena/Trino.
-- =============================================================================

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2007-01-01' as date)",
            end_date="cast('2020-12-31' as date)"
        )
    }}

),

enriched as (

    select
        -- Surrogate key: YYYYMMDD como integer para joins rapidos.
        -- Ejemplo: 2018-03-15 -> 20180315.
        cast(date_format(date_day, '%Y%m%d') as integer)    as date_key,

        -- Fecha en formato nativo (para display).
        date_day                                             as full_date,

        -- Componentes basicos
        year(date_day)                                       as year,
        quarter(date_day)                                    as quarter,
        month(date_day)                                      as month,
        day_of_month(date_day)                               as day_of_month,
        day_of_week(date_day)                                as day_of_week,
        day_of_year(date_day)                                as day_of_year,
        week_of_year(date_day)                               as week_of_year,

        -- Labels legibles (utiles para dashboards)
        date_format(date_day, '%M')                          as month_name,
        date_format(date_day, '%W')                          as day_name,
        concat(cast(year(date_day) as varchar), '-Q', cast(quarter(date_day) as varchar)) as year_quarter,
        date_format(date_day, '%Y-%m')                       as year_month,

        -- Flags utiles
        case
            when day_of_week(date_day) in (6, 7) then true
            else false
        end                                                  as is_weekend,

        case
            when date_day = last_day_of_month(date_day) then true
            else false
        end                                                  as is_month_end,

        case
            when month(date_day) = 12 and day_of_month(date_day) = 31 then true
            else false
        end                                                  as is_year_end

    from date_spine

)

select * from enriched