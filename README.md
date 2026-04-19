# Pipeline ELT CFPB — Fundamentos de Engenharia de Dados

**Domínio:** Finanças  
**Dataset:** [CFPB Consumer Finance Complaints](https://www.kaggle.com/datasets/cfpb/us-consumer-finance-complaints) (~500k registros)  
**Tecnologias:** Python · PostgreSQL · Great Expectations · dbt · Apache Airflow · Metabase · Docker

---

## Storytelling e Perguntas de Negócio

Você é Analista de Dados Sênior do Departamento de Compliance de uma instituição financeira. Reclamações públicas registradas no CFPB (Consumer Financial Protection Bureau) aumentaram 23% nos últimos 6 meses. A diretoria precisa de um painel atualizado diariamente para responder:

1. Quais produtos financeiros geram mais reclamações?
2. Qual o percentual de respostas tempestivas (menos de 15 dias)?
3. Qual a taxa de disputas após a resposta da empresa?
4. Existe concentração geográfica nos problemas reportados?
5. Há problemas de qualidade nos dados recebidos?

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│               CFPB CSV (500K+ registros)                     │
│     https://www.kaggle.com/datasets/cfpb/us-consumer-finance-complaints
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│         SCRIPT PYTHON CUSTOMIZADO (ingest_cfpb.py)           │
│  • Leitura em chunks (10k/batch)                             │
│  • INSERT em massa via psycopg2.execute_batch                │
│  • Upsert idempotente (ON CONFLICT)                          │
│  • Logging estruturado e métricas de performance             │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│          POSTGRESQL — SCHEMA RAW (camada bronze)             │
│       Tabela: raw.consumer_complaints (18 colunas)           │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│          GREAT EXPECTATIONS (14 expectativas)                │
│       Checkpoint: daily_validation                           │
│       Suite: raw_complaints_suite                            │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│          DBT — Silver → Gold (Star Schema)                   │
│  • stg_complaints (VIEW no schema silver)                    │
│  • dim_product · dim_company · dim_geography · dim_date      │
│  • fact_complaints                                           │
│  • 2 macros customizadas · 2 testes singulares               │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│          APACHE AIRFLOW (8 tasks orquestradas)               │
│       DAG: cfpb_complaints_daily_pipeline                    │
│       Schedule: diário às 03:00 · retries: 2                 │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│          METABASE — Dashboards (schema gold)                 │
│       http://localhost:3001                                  │
└──────────────────────────────────────────────────────────────┘
```

---

## Passo a Passo para Executar o Pipeline

### Pré-requisitos

- Docker e Docker Compose instalados
- Conta no [Kaggle](https://www.kaggle.com) com token de API gerado

### 1. Clonar o repositório

```bash
git clone https://github.com/hrvfreitas/eEDB-019-2026-1.git
cd eEDB-019-2026-1
```

### 2. Configurar credenciais do Kaggle

```bash
cp .env.example .env
# Edite o .env com seu usuário e token do Kaggle
# Obtenha em: https://www.kaggle.com/settings -> API -> Create New Token
```

### 3. Baixar o dataset

```bash
# Opção A: via script automatizado
docker-compose run ingestion python /scripts/download_kaggle.py

# Opção B: manual
# Acesse https://www.kaggle.com/datasets/cfpb/us-consumer-finance-complaints
# Baixe o arquivo CSV e salve em: data/complaints.csv
```

### 4. Subir o ambiente

```bash
docker-compose up -d
```

Aguarde todos os serviços ficarem saudáveis (~2 minutos):

```bash
docker-compose ps
```

### 5. Executar o pipeline

**Via Airflow (recomendado):**

1. Acesse http://localhost:8080 (usuário: `admin` / senha: `admin`)
2. Ative a DAG `cfpb_complaints_daily_pipeline`
3. Clique em "Trigger DAG" para executar manualmente

**Via linha de comando:**

```bash
# Ingestão
docker-compose run ingestion python /scripts/ingest_cfpb.py

# Great Expectations
docker exec projeto_airflow_webserver bash -c \
  "cd /opt/airflow/great_expectations && great_expectations checkpoint run daily_validation"

# dbt
docker-compose run dbt dbt deps --profiles-dir /usr/app/profiles
docker-compose run dbt dbt run --profiles-dir /usr/app/profiles
docker-compose run dbt dbt test --profiles-dir /usr/app/profiles
docker-compose run dbt dbt docs generate --profiles-dir /usr/app/profiles
```

### 6. Acessar dashboards

Acesse http://localhost:3001 e conecte ao banco:

- **Host:** postgres
- **Porta:** 5432
- **Database:** projeto
- **Usuário:** postgres
- **Senha:** postgres
- **Schema:** gold

---

## Modelo de Dados (Star Schema)

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  date_sk (PK)│
                    └──────┬───────┘
                           │
┌─────────────┐    ┌───────┴──────────┐    ┌──────────────┐
│ dim_product │    │  fact_complaints  │    │ dim_company  │
│ product_sk  │◄───│  complaint_sk(PK) │───►│ company_sk   │
│ product_name│    │  product_sk (FK)  │    │ company_name │
│ sub_product │    │  company_sk (FK)  │    │ company_resp │
│ issue       │    │  geography_sk(FK) │    └──────────────┘
│ sub_issue   │    │  date_received_sk │
└─────────────┘    │  response_days    │    ┌──────────────┐
                   │  timely_response  │    │dim_geography │
                   │  consumer_disputed│───►│ geography_sk │
                   └──────────────────┘    │ state        │
                                           │ region       │
                                           │ zip_code     │
                                           └──────────────┘
```

---

## Capturas de Tela dos Dashboards

> Adicionar screenshots após configurar o Metabase.

Dashboards sugeridos:

1. **Executive Overview** — volume total de reclamações por mês e produto
2. **Response Performance** — % de respostas tempestivas e média de dias
3. **Geographic Heatmap** — concentração de reclamações por estado e região
4. **Dispute Analysis** — taxa de disputas por empresa e tipo de resposta

---

## Estrutura do Repositório

```
eEDB-019-2026-1/
├── docker-compose.yml              # 9 serviços containerizados
├── .env.example                    # Template de credenciais
├── init_db/
│   ├── 01_init_schemas.sql         # Cria banco airflow
│   └── 02_init_schemas.sql         # Cria schemas raw/silver/gold
├── scripts/
│   ├── ingest_cfpb.py              # Script de ingestão Python customizado
│   ├── download_kaggle.py          # Download automático do dataset
│   └── Dockerfile.ingestion        # Container de ingestão
├── airflow/
│   └── dags/
│       └── cfpb_daily_pipeline.py  # DAG com 8 tasks
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles/profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_complaints.sql
│   │   └── gold/
│   │       ├── schema.yml
│   │       ├── dimensions/
│   │       │   ├── dim_company.sql
│   │       │   ├── dim_date.sql
│   │       │   ├── dim_geography.sql
│   │       │   └── dim_product.sql
│   │       └── facts/
│   │           └── fact_complaints.sql
│   ├── macros/
│   │   ├── classify_response_timeliness.sql
│   │   └── calculate_region.sql
│   └── tests/singular/
│       ├── test_timely_response_accuracy.sql
│       └── test_date_logic.sql
└── great_expectations/
    ├── great_expectations.yml
    ├── expectations/
    │   └── raw_complaints_suite.json
    └── checkpoints/
        └── daily_validation.yml
```

---

## Troubleshooting

**Erro: "File not found" (complaints.csv)**
```bash
ls -lh data/complaints.csv
# Se não existir, execute o download (passo 3)
```

**Erro: "Connection refused" (PostgreSQL)**
```bash
docker-compose ps postgres
docker-compose restart postgres
```

**Airflow: usuário admin não existe**
```bash
docker exec projeto_airflow_webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

**Verificar dados no PostgreSQL**
```bash
docker exec -it projeto_postgres psql -U postgres -d projeto \
  -c "SELECT COUNT(*) FROM raw.consumer_complaints;"
```
