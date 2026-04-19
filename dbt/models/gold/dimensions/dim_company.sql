{{
    config(materialized='table', schema='gold')
}}

WITH companies AS (
    SELECT DISTINCT company_name, company_response
    FROM {{ ref('stg_complaints') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['company_name','company_response']) }} AS company_sk,
    company_name, company_response,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM companies
