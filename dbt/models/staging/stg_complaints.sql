{{
    config(
        materialized='view',
        schema='silver'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'consumer_complaints') }}
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['complaint_id']) }} AS complaint_sk,
        CAST(complaint_id AS BIGINT) AS complaint_id,
        CAST(date_received AS DATE) AS date_received,
        CAST(date_sent_to_company AS DATE) AS date_sent_to_company,
        COALESCE(NULLIF(TRIM(product), ''), 'Not Specified') AS product,
        COALESCE(NULLIF(TRIM(sub_product), ''), 'Not Specified') AS sub_product,
        COALESCE(NULLIF(TRIM(issue), ''), 'Not Specified') AS issue,
        COALESCE(NULLIF(TRIM(sub_issue), ''), 'Not Specified') AS sub_issue,
        UPPER(TRIM(company)) AS company_name,
        COALESCE(NULLIF(TRIM(company_response_to_consumer), ''), 'Not Specified') AS company_response,
        UPPER(TRIM(state)) AS state,
        LPAD(TRIM(zip_code), 5, '0') AS zip_code,
        CASE WHEN UPPER(timely_response) = 'YES' THEN TRUE
             WHEN UPPER(timely_response) = 'NO' THEN FALSE
             ELSE NULL END AS timely_response_flag,
        CASE WHEN UPPER(consumer_disputed) = 'YES' THEN TRUE
             WHEN UPPER(consumer_disputed) = 'NO' THEN FALSE
             ELSE NULL END AS consumer_disputed_flag,
        CASE WHEN consumer_consent_provided = 'Consent provided'
                  AND consumer_complaint_narrative IS NOT NULL
             THEN TRUE ELSE FALSE END AS has_narrative_flag,
        consumer_complaint_narrative AS complaint_narrative,
        COALESCE(NULLIF(TRIM(submitted_via), ''), 'Unknown') AS submitted_via,
        COALESCE(NULLIF(TRIM(tags), ''), 'None') AS tags,
        CASE WHEN date_sent_to_company IS NOT NULL AND date_received IS NOT NULL
             THEN DATE_PART('day', CAST(date_sent_to_company AS DATE) - CAST(date_received AS DATE))
             ELSE NULL END AS response_days,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE complaint_id IS NOT NULL
      AND date_received IS NOT NULL
      AND product IS NOT NULL
      AND company IS NOT NULL
)

SELECT * FROM cleaned
