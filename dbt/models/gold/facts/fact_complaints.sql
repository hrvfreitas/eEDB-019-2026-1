{{
    config(materialized='table', schema='gold')
}}

WITH complaints AS (
    SELECT * FROM {{ ref('stg_complaints') }}
),
dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
),
dim_company AS (
    SELECT * FROM {{ ref('dim_company') }}
),
dim_geography AS (
    SELECT * FROM {{ ref('dim_geography') }}
)

SELECT
    c.complaint_sk,
    c.complaint_id,
    dp.product_sk,
    dc.company_sk,
    dg.geography_sk,
    CAST(TO_CHAR(c.date_received, 'YYYYMMDD') AS INTEGER) AS date_received_sk,
    CAST(TO_CHAR(c.date_sent_to_company, 'YYYYMMDD') AS INTEGER) AS date_sent_to_company_sk,
    c.date_received,
    c.date_sent_to_company,
    c.response_days,
    {{ classify_response_timeliness('c.response_days') }} AS response_timeliness_category,
    c.timely_response_flag,
    c.consumer_disputed_flag,
    c.has_narrative_flag,
    c.submitted_via,
    c.tags,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM complaints c
LEFT JOIN dim_product dp
    ON {{ dbt_utils.generate_surrogate_key(['c.product','c.sub_product','c.issue','c.sub_issue']) }} = dp.product_sk
LEFT JOIN dim_company dc
    ON {{ dbt_utils.generate_surrogate_key(['c.company_name','c.company_response']) }} = dc.company_sk
LEFT JOIN dim_geography dg
    ON {{ dbt_utils.generate_surrogate_key(['c.state','c.zip_code']) }} = dg.geography_sk
