{%- macro classify_response_timeliness(response_days) -%}
    CASE
        WHEN {{ response_days }} IS NULL OR {{ response_days }} < 0 THEN 'Unknown'
        WHEN {{ response_days }} BETWEEN 0 AND 3 THEN 'Immediate'
        WHEN {{ response_days }} BETWEEN 4 AND 7 THEN 'Fast'
        WHEN {{ response_days }} BETWEEN 8 AND 15 THEN 'Timely'
        ELSE 'Late'
    END
{%- endmacro -%}
