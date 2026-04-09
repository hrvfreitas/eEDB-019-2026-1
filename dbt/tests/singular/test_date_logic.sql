SELECT
    complaint_id, date_received, date_sent_to_company
FROM {{ ref('fact_complaints') }}
WHERE date_sent_to_company < date_received
