#!/usr/bin/env python3
"""
Script Master - Gera todos os arquivos do projeto CFPB v2
Versão SEM Airbyte (com script Python customizado)
"""

import os
from pathlib import Path

BASE_DIR = Path("/home/claude/projeto_cfpb_v2")

# Dicionário com TODOS os arquivos
FILES = {
    # ==================== INIT DB ====================
    "init_db/01_init_schemas.sql": """-- Script de Inicialização PostgreSQL
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA raw IS 'Bronze Layer - Dados brutos do script Python';
COMMENT ON SCHEMA silver IS 'Silver Layer - Dados limpos (staging dbt)';
COMMENT ON SCHEMA gold IS 'Gold Layer - Star Schema para analytics';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

GRANT USAGE ON SCHEMA raw TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;

DO $$
BEGIN
    RAISE NOTICE 'Schemas criados: raw, silver, gold';
END $$;
""",

    # ==================== DBT PROJECT ====================
    "dbt/dbt_project.yml": """name: 'cfpb_complaints'
version: '1.0.0'
config-version: 2

profile: 'cfpb'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"
  - "logs"

vars:
  start_date: '2015-01-01'
  timely_response_threshold_days: 15

models:
  cfpb_complaints:
    +materialized: table
    +schema: silver
    
    staging:
      +materialized: view
      +schema: silver
      +tags: ["staging", "silver"]
    
    gold:
      +materialized: table
      +schema: gold
      +tags: ["gold", "analytics"]
""",

    "dbt/packages.yml": """packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.1
""",

    "dbt/profiles/profiles.yml": """cfpb:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      port: 5432
      user: postgres
      password: postgres
      dbname: projeto
      schema: silver
      threads: 4
""",

    # ==================== DBT MACROS ====================
    "dbt/macros/classify_response_timeliness.sql": """{%- macro classify_response_timeliness(response_days) -%}
    CASE
        WHEN {{ response_days }} IS NULL OR {{ response_days }} < 0 THEN 'Unknown'
        WHEN {{ response_days }} BETWEEN 0 AND 3 THEN 'Immediate'
        WHEN {{ response_days }} BETWEEN 4 AND 7 THEN 'Fast'
        WHEN {{ response_days }} BETWEEN 8 AND 15 THEN 'Timely'
        ELSE 'Late'
    END
{%- endmacro -%}
""",

    "dbt/macros/calculate_region.sql": """{%- macro calculate_region(state_column) -%}
    CASE
        WHEN {{ state_column }} IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
        WHEN {{ state_column }} IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
        WHEN {{ state_column }} IN ('DE','FL','GA','MD','NC','SC','VA','WV','AL','KY','MS','TN','AR','LA','OK','TX','DC') THEN 'South'
        WHEN {{ state_column }} IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
        WHEN {{ state_column }} IN ('AS','GU','MP','PR','VI','UM','FM','MH','PW') THEN 'Territories'
        ELSE 'Unknown'
    END
{%- endmacro -%}
""",

    # ==================== DBT STAGING ====================
    "dbt/models/staging/sources.yml": """version: 2

sources:
  - name: raw
    description: Schema RAW com dados do script Python
    database: projeto
    schema: raw
    tables:
      - name: consumer_complaints
        description: Reclamações CFPB carregadas via script Python
        columns:
          - name: complaint_id
            tests:
              - unique
              - not_null
          - name: date_received
            tests:
              - not_null
""",

    "dbt/models/staging/stg_complaints.sql": """{{
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
""",

    # ==================== DBT GOLD DIMENSIONS ====================
    "dbt/models/gold/dimensions/dim_date.sql": """{{
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
""",

    "dbt/models/gold/dimensions/dim_product.sql": """{{
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
""",

    "dbt/models/gold/dimensions/dim_company.sql": """{{
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
""",

    "dbt/models/gold/dimensions/dim_geography.sql": """{{
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
""",

    # ==================== DBT GOLD FACTS ====================
    "dbt/models/gold/facts/fact_complaints.sql": """{{
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
""",

    # ==================== DBT TESTS ====================
    "dbt/tests/singular/test_date_logic.sql": """SELECT
    complaint_id, date_received, date_sent_to_company
FROM {{ ref('fact_complaints') }}
WHERE date_sent_to_company < date_received
""",

    "dbt/tests/singular/test_timely_response_accuracy.sql": """SELECT
    complaint_id, response_days, timely_response_flag
FROM {{ ref('fact_complaints') }}
WHERE (response_days <= 15 AND timely_response_flag = FALSE)
   OR (response_days > 15 AND timely_response_flag = TRUE)
""",

    "dbt/models/gold/schema.yml": """version: 2

models:
  - name: fact_complaints
    columns:
      - name: complaint_sk
        tests:
          - unique
          - not_null
      - name: product_sk
        tests:
          - relationships:
              to: ref('dim_product')
              field: product_sk
""",

    # ==================== GREAT EXPECTATIONS ====================
    "great_expectations/great_expectations.yml": """config_version: 3.0

datasources:
  cfpb_postgres:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: postgresql://postgres:postgres@postgres:5432/projeto
    data_connectors:
      default_inferred_data_connector:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: true

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/
""",
}

# Criar arquivos
print("="*60)
print("🚀 CRIANDO ARQUIVOS DO PROJETO CFPB v2")
print("="*60)

for filepath, content in FILES.items():
    full_path = BASE_DIR / filepath
    full_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(full_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✓ {filepath}")

print(f"\n✅ {len(FILES)} arquivos criados com sucesso!")
print(f"📁 Diretório base: {BASE_DIR}")
