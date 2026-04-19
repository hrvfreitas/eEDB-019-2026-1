{{
    config(materialized='table', schema='gold')
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2011-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INTEGER) AS date_sk,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    TO_CHAR(date_day, 'Month') AS month_name,
    CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM date_spine
