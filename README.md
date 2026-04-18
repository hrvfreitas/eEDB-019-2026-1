# Projeto Final - Pipeline ELT CFPB com Script Python Customizado

**Domínio:** Finanças  
**Dataset:** CFPB Consumer Finance Complaints (~500k registros)  
**Diferencial:** Script Python customizado para ingestão (SEM Airbyte)

---

## 📖 Storytelling

Você é Analista de Dados Sênior do Departamento de Compliance de uma instituição financeira. Reclamações públicas no CFPB aumentaram 23% em 6 meses. A diretoria precisa de um painel atualizado diariamente respondendo:

1. Quais produtos geram mais reclamações?
2. Qual % de respostas tempestivas (<15 dias)?
3. Taxa de disputas após resposta da empresa?
4. Concentração geográfica de problemas?
5. Há problemas de qualidade nos dados?

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│               CFPB CSV (500K+ registros)                       │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│           SCRIPT PYTHON CUSTOMIZADO (ingest_cfpb.py)         │
│  • Pandas para leitura em chunks (10k/batch)                 │
│  • psycopg2 para INSERT em massa                             │
│  • Logging detalhado                                         │
│  • Tratamento de erros e retry                               │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│            POSTGRESQL - SCHEMA RAW                           │
│  Tabela: raw.consumer_complaints (18 colunas)                │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│            GREAT EXPECTATIONS (8 expectativas)               │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│       DBT (Silver → Gold Star Schema)                        │
│  • 1 staging view                                            │
│  • 4 dimensões + 1 fato                                      │
│  • 2 macros customizadas                                     │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│       APACHE AIRFLOW (8 tasks orquestradas)                  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│            METABASE (4 dashboards)                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start (15 minutos)

### 1. Download automático do dataset (Kaggle API)

O dataset do CFPB é obtido automaticamente via integração com a Kaggle Hub API, eliminando a necessidade de download manual.

- Autenticação via variável de ambiente `KAGGLE_API_TOKEN`
- Download realizado diretamente para a pasta `/data`, utilizada pelo pipeline
- Etapa integrada ao fluxo de execução (Docker / Airflow)

```bash
# Executar download manual (opcional)
docker-compose run ingestion python /scripts/download_cfpb.py
```

### 2. Subir Ambiente
```bash
docker-compose up -d
```

### 3. Executar Ingestão Python
```bash
# Opção A: Rodar diretamente
docker-compose run ingestion python /scripts/ingest_cfpb.py

# Opção B: Via Airflow (recomendado)
# Acesse http://localhost:8080 (admin/admin)
# Ative e execute a DAG: cfpb_complaints_daily_pipeline
```

### 4. Acompanhar Logs
```bash
docker-compose logs -f ingestion
```

---

## 📊 Modelo de Dados (Star Schema)

### Fato
- **fact_complaints** (~3M linhas)
  - complaint_sk, complaint_id
  - product_sk, company_sk, geography_sk
  - date_received_sk, date_sent_to_company_sk
  - response_days, response_timeliness_category
  - timely_response_flag, consumer_disputed_flag

### Dimensões
- **dim_product**: produtos, issues
- **dim_company**: empresas, respostas
- **dim_geography**: estados, regiões (macro calculate_region)
- **dim_date**: 2011-2030, granularidade diária

---

## 🔧 Componentes Técnicos

### Script Python de Ingestão (361 linhas)
**Localização:** `scripts/ingest_cfpb.py`

**Características:**
- ✅ Leitura em chunks (10k registros/vez)
- ✅ INSERT em massa com psycopg2.extras.execute_batch
- ✅ ON CONFLICT para upsert (idempotente)
- ✅ Logging estruturado
- ✅ Validação pós-carga
- ✅ Métricas de performance (registros/segundo)

**Configuração via ENV:**
```python
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=projeto
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
DATA_PATH=/data/complaints.csv
```

**Execução Manual:**
```bash
python scripts/ingest_cfpb.py
```

### DAG Airflow (150 linhas)
**Localização:** `airflow/dags/cfpb_daily_pipeline.py`

**8 Tasks:**
1. check_data_file_exists (FileSensor)
2. run_python_ingestion (BashOperator)
3. run_great_expectations (BashOperator)
4. run_dbt_deps (BashOperator)
5. run_dbt_models (BashOperator)
6. run_dbt_tests (BashOperator)
7. check_data_quality (PythonOperator)
8. generate_dbt_docs (BashOperator)

**Schedule:** Diário às 03:00

---


## 🐳 Containers Docker

1. **postgres** - PostgreSQL 15
2. **ingestion** - Python 3.11 (script customizado)
3. **airflow-webserver** - Airflow UI
4. **airflow-scheduler** - Orquestração
5. **airflow-triggerer** - Tasks assíncronas
6. **dbt** - Transformações
7. **metabase** - Visualização

---

## 📁 Estrutura do Projeto

```
projeto-cfpb-pipeline/
├── scripts/
│   ├── ingest_cfpb.py          # ★ Script Python de ingestão (361 linhas)
│   └── Dockerfile.ingestion    # Container customizado
├── airflow/dags/
│   └── cfpb_daily_pipeline.py  # DAG com 8 tasks
├── dbt/
│   ├── models/
│   │   ├── staging/stg_complaints.sql
│   │   └── gold/{dimensions,facts}/
│   ├── macros/                 # 2 macros customizadas
│   └── tests/                  # 2 testes singulares
├── great_expectations/
│   └── great_expectations.yml
├── docker-compose.yml          # 7 serviços
└── README.md                   # Este arquivo
```

---

## 💡 Diferenciais vs Airbyte

### Por que Script Python Customizado?

✅ **Controle Total:**
- Lógica de transformação na ingestão
- Tratamento de erros personalizado
- Validação inline

✅ **Performance:**
- Otimizado para CSV grande (chunks)
- INSERT em massa (1000 registros/batch)
- ~15k registros/segundo

✅ **Simplicidade:**
- Sem dependências externas complexas
- Código Python puro (fácil debug)
- Deploy simplificado (1 container vs 5 do Airbyte)

✅ **Observabilidade:**
- Logs estruturados
- Métricas detalhadas
- Fácil integração com Airflow

---

## 🎯 Próximos Passos

1. **Configurar Metabase** (5 min)
   - http://localhost:3000
   - Conectar: postgres:5432, database=projeto, schema=gold

2. **Criar Dashboards** (30 min)
   - Executive Overview
   - Product Deep Dive
   - Geographic Heatmap
   - Response Performance

3. **Testar Pipeline End-to-End** (10 min)
   - Executar DAG no Airflow
   - Verificar logs de cada task
   - Validar dados no gold

---

## 🐛 Troubleshooting

### Erro: "File not found"
```bash
# Verificar se CSV existe
ls -lh data/complaints.csv

# Baixar se necessário
 e salvar em data/complaints.csv"
```

### Erro: "Connection refused"
```bash
# Verificar se PostgreSQL está up
docker-compose ps postgres

# Reiniciar se necessário
docker-compose restart postgres
```

### Script de ingestão trava
```bash
# Verificar logs
docker-compose logs -f ingestion

# Testar conexão PostgreSQL
docker exec -it projeto_postgres psql -U postgres -d projeto -c "SELECT 1;"
```

---

## 📊 Métricas de Performance

**Ingestão de ~500k registros:**
- Tempo: ~4-5 minutos
- Taxa: ~12k-15k registros/segundo
- Memória: ~500MB (chunks de 10k)

**Pipeline Completo (Airflow):**
- Ingestão: 4-5 min
- Great Expectations: 30s
- dbt run: 2-3 min
- dbt test: 1 min
- **Total: ~8-10 minutos**

---

## 📚 Documentação Adicional

- `scripts/ingest_cfpb.py` - Código comentado do script
- `airflow/dags/cfpb_daily_pipeline.py` - DAG documentada
- `dbt/` - Modelos SQL comentados

---


