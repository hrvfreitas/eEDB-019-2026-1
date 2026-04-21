{{ config(severity='warn') }}

SELECT
    complaint_id, response_days, timely_response_flag
FROM {{ ref('fact_complaints') }}
WHERE (response_days <= 15 AND timely_response_flag = FALSE)
   OR (response_days > 15 AND timely_response_flag = TRUE)
