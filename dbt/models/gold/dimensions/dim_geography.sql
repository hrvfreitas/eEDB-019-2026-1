{{
    config(materialized='table', schema='gold')
}}

WITH geography AS (
    SELECT DISTINCT state, zip_code
    FROM {{ ref('stg_complaints') }}
    WHERE state IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['state','zip_code']) }} AS geography_sk,
    state,
    {{ calculate_region('state') }} AS region,
    zip_code,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM geography
