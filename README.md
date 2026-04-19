# 🚀 eEDB-019-2026-1 - Pipeline CFPB Completo

**Projeto Final Integrado** - Great Expectations (8 validações) + Relatório HTML + Consultas Metabase + dbt Docs

---

## ⚡ Quick Start

### **Opção A: Dados REAIS do Kaggle 

```bash
# 1. Configure credenciais Kaggle (veja KAGGLE_SETUP.md)
export KAGGLE_USERNAME='hrvfreitas'
export KAGGLE_KEY='sua_chave_api'

# 2. Suba o ambiente
docker-compose up -d

# 3. Baixe dataset CFPB do Kaggle (~500k registros)
./download_kaggle_data.sh

# 4. Execute ingestão
docker-compose run --rm ingestion python /scripts/ingest_cfpb.py

# 5. Execute pipeline completo
python3 run_full_pipeline.py
```

### **Opção B: Dados de TESTE (1000 registros fake)** 🧪

```bash
# 1. Suba o ambiente
docker-compose up -d

# 2. Crie dados de exemplo
./create_sample_data.sh

# 3. Execute pipeline completo
python3 run_full_pipeline.py
```

**Acesse:**
- 📊 dbt Docs + Lineage: http://localhost:8001
- 🔄 Airflow: http://localhost:8080 (admin/admin)
- 📈 Metabase: http://localhost:3000
- 📄 Relatório GE: `great_expectations_report.html`

---

## 📋 8 Validações Great Expectations (E1-E8)

| # | Campo | Validação | Descrição |
|---|-------|-----------|-----------|
| **E1** | complaint_id | Not Null | Chave primária obrigatória |
| **E2** | complaint_id | Unique | Sem duplicatas |
| **E3** | product | In Set | Apenas produtos válidos |
| **E4** | date_received | Valid Date | Formato de data correto |
| **E5** | state | 2 chars | Sigla de estado americana |
| **E6** | timely_response | Yes/No | Campo booleano textual |
| **E7** | submitted_via | In Set | Canais de envio predefinidos |
| **E8** | company | Not Null | Empresa obrigatória |

---

## 📊 4 Consultas Metabase (A-D)

**A** - Top 10 produtos por volume  
**B** - Top 10 empresas por reclamações  
**C** - Pior taxa de resposta por empresa  
**D** - Issues por região geográfica

Queries completas em: `metabase_queries.sql`

---

## 🎯 Pipeline Automatizado

```bash
python3 run_full_pipeline.py
```

**Executa:**
1. ✅ Valida conexão PostgreSQL
2. ✅ Verifica dados RAW
3. ✅ Executa Great Expectations (8 validações)
4. ✅ Gera relatório HTML customizado
5. ✅ Roda transformações dbt (Silver + Gold)
6. ✅ Testa consultas Metabase
7. ✅ Mostra sumário final

---

## 📁 Estrutura do Projeto

```
eEDB-019-2026-1-clean/
├── docker-compose.yml              # 5 containers integrados
├── run_full_pipeline.py            # ⭐ Pipeline completo
├── create_sample_data.sh           # Dados de teste
├── metabase_queries.sql            # 4 consultas (A-D)
├── great_expectations/
│   ├── create_suite.py             # 8 validações (E1-E8)
│   └── generate_html_report.py    # Relatório HTML
├── dbt/models/
│   ├── staging/stg_complaints.sql  # Silver
│   └── gold/                       # Star Schema (5 tabelas)
└── airflow/dags/                   # Orquestração
```

---

## 🐳 Containers (5 total)

| Container | Porta | Descrição |
|-----------|-------|-----------|
| postgres | 5432 | PostgreSQL 16 (raw/silver/gold) |
| airflow | 8080 | Airflow all-in-one |
| **dbt** | **8001** | **Transformações + Docs Server** ⭐ |
| metabase | 3000 | BI dashboards |
| ingestion | - | Python data loader (on-demand) |

---

## 📖 Documentação Completa

**Autores:** Daniely Luz Santos · Diane de Paula Santos · Hercules Ramos Veloso de Freitas  
**Curso:** eEDB-019-2026-1 - Fundamentos de Engenharia de Dados  
**Instituição:** Escola Politécnica da USP - Especialização Big Data  

**GitHub:** github.com/hrvfreitas/eEDB-019-2026-1 | Abril 2026

---

## 📝 Licença

GNU Licence