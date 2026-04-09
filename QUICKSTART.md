# ⚡ Quick Start - 15 Minutos

## Comandos Rápidos

```bash
# 1. Baixar dataset e salvar em data/complaints.csv"

# 2. Subir Docker
docker-compose up -d

# 3. Executar ingestão Python
docker-compose run ingestion python /scripts/ingest_cfpb.py

# 4. Executar Great Expectations
docker exec -it projeto_airflow_webserver bash
pip install great-expectations
# (criar suite conforme great_expectations/create_suite.py)

# 5. Executar dbt
docker-compose run dbt deps
docker-compose run dbt run
docker-compose run dbt test

# 6. Airflow: http://localhost:8080 (admin/admin)
# Ativar DAG: cfpb_complaints_daily_pipeline

# 7. Metabase: http://localhost:3000
# Conectar PostgreSQL gold
```

## URLs

- **Airflow**: http://localhost:8080 (admin/admin)
- **Metabase**: http://localhost:3000
- **PostgreSQL**: localhost:5432 (postgres/postgres)

## Validação

```bash
# Verificar dados no raw
docker exec -it projeto_postgres psql -U postgres -d projeto \
  -c "SELECT COUNT(*) FROM raw.consumer_complaints;"

# Verificar gold
docker exec -it projeto_postgres psql -U postgres -d projeto \
  -c "SELECT COUNT(*) FROM gold.fact_complaints;"
```

**Tempo total:** 10-15 minutos
