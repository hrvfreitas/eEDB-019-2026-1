[CSV] → Airbyte (File Source) → PostgreSQL (raw) 
    ↓
Great Expectations (validação na raw)
    ↓
dbt (raw → silver → gold com modelo estrela)
    ↓
Airflow (orquestração)
    ↓
Metabase/Superset (dashboards)
