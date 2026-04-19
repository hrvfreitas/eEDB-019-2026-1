{{
    config(materialized='table', schema='gold')
}}

WITH products AS (
    SELECT DISTINCT
        product, sub_product, issue, sub_issue
    FROM {{ ref('stg_complaints') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product','sub_product','issue','sub_issue']) }} AS product_sk,
    product AS product_name,
    sub_product, issue, sub_issue,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM products
