#!/usr/bin/env python3
"""
Script de Ingestão CFPB - Substituto do Airbyte
Extrai dados do CSV e carrega no PostgreSQL (schema raw)

Autor: Hercules (NUSP: 5479786)
Projeto: PECE Big Data - Fundamentos de Engenharia de Dados
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime
import logging
from pathlib import Path

# ============================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/ingest_cfpb.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÕES
# ============================================================
class Config:
    """Configurações de conexão e paths"""
    # PostgreSQL
    DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
    DB_PORT = os.getenv('POSTGRES_PORT', '5432')
    DB_NAME = os.getenv('POSTGRES_DB', 'projeto')
    DB_USER = os.getenv('POSTGRES_USER', 'postgres')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    DB_SCHEMA = 'raw'
    DB_TABLE = 'consumer_complaints'
    
    # Dados
    DATA_PATH = os.getenv('DATA_PATH', '/data/complaints.csv')
    CHUNK_SIZE = 10000  # Processar 10k linhas por vez
    
    # Mapeamento de colunas CSV → PostgreSQL
    COLUMN_MAPPING = {
        'Date received': 'date_received',
        'Product': 'product',
        'Sub-product': 'sub_product',
        'Issue': 'issue',
        'Sub-issue': 'sub_issue',
        'Consumer complaint narrative': 'consumer_complaint_narrative',
        'Company public response': 'company_public_response',
        'Company': 'company',
        'State': 'state',
        'ZIP code': 'zip_code',
        'Tags': 'tags',
        'Consumer consent provided?': 'consumer_consent_provided',
        'Submitted via': 'submitted_via',
        'Date sent to company': 'date_sent_to_company',
        'Company response to consumer': 'company_response_to_consumer',
        'Timely response?': 'timely_response',
        'Consumer disputed?': 'consumer_disputed',
        'Complaint ID': 'complaint_id'
    }

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def get_db_connection():
    """Cria conexão com PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        logger.info("✓ Conexão com PostgreSQL estabelecida")
        return conn
    except Exception as e:
        logger.error(f"✗ Erro ao conectar no PostgreSQL: {e}")
        raise

def create_table(conn):
    """
    Cria tabela raw.consumer_complaints se não existir
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw.consumer_complaints (
        complaint_id BIGINT PRIMARY KEY,
        date_received DATE,
        product VARCHAR(255),
        sub_product VARCHAR(255),
        issue VARCHAR(500),
        sub_issue VARCHAR(500),
        consumer_complaint_narrative TEXT,
        company_public_response TEXT,
        company VARCHAR(255),
        state VARCHAR(2),
        zip_code VARCHAR(10),
        tags VARCHAR(100),
        consumer_consent_provided VARCHAR(50),
        submitted_via VARCHAR(50),
        date_sent_to_company DATE,
        company_response_to_consumer VARCHAR(255),
        timely_response VARCHAR(10),
        consumer_disputed VARCHAR(10),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Índices para performance
    CREATE INDEX IF NOT EXISTS idx_complaint_id ON raw.consumer_complaints(complaint_id);
    CREATE INDEX IF NOT EXISTS idx_date_received ON raw.consumer_complaints(date_received);
    CREATE INDEX IF NOT EXISTS idx_company ON raw.consumer_complaints(company);
    CREATE INDEX IF NOT EXISTS idx_product ON raw.consumer_complaints(product);
    CREATE INDEX IF NOT EXISTS idx_state ON raw.consumer_complaints(state);
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
        logger.info("✓ Tabela raw.consumer_complaints criada/verificada")
    except Exception as e:
        logger.error(f"✗ Erro ao criar tabela: {e}")
        conn.rollback()
        raise

def truncate_table(conn):
    """
    Trunca tabela para Full Refresh (equivalente ao Airbyte Full Refresh - Overwrite)
    """
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw.consumer_complaints;")
            conn.commit()
        logger.info("✓ Tabela truncada (Full Refresh)")
    except Exception as e:
        logger.error(f"✗ Erro ao truncar tabela: {e}")
        conn.rollback()
        raise

def clean_dataframe(df):
    """
    Limpa e prepara DataFrame para inserção
    """
    # Renomear colunas conforme mapeamento
    df = df.rename(columns=Config.COLUMN_MAPPING)
    
    # Remover colunas que não estão no mapeamento
    expected_cols = list(Config.COLUMN_MAPPING.values())
    df = df[[col for col in df.columns if col in expected_cols]]
    
    # Converter datas
    date_cols = ['date_received', 'date_sent_to_company']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Limpar ZIP codes (remover sufixos)
    if 'zip_code' in df.columns:
        df['zip_code'] = df['zip_code'].astype(str).str[:5]
        df['zip_code'] = df['zip_code'].replace('nan', None)
    
    # Converter complaint_id para inteiro
    if 'complaint_id' in df.columns:
        df['complaint_id'] = pd.to_numeric(df['complaint_id'], errors='coerce')
        df = df.dropna(subset=['complaint_id'])  # Remover linhas sem ID
        df['complaint_id'] = df['complaint_id'].astype(int)
    
    # Substituir NaN por None (NULL no PostgreSQL)
    df = df.where(pd.notnull(df), None)
    
    return df

def insert_batch(conn, df, batch_num):
    """
    Insere batch de dados usando COPY (mais rápido) ou INSERT em massa
    """
    try:
        # Preparar dados para inserção
        records = df.to_dict('records')
        
        # Colunas na ordem correta
        columns = [
            'complaint_id', 'date_received', 'product', 'sub_product',
            'issue', 'sub_issue', 'consumer_complaint_narrative',
            'company_public_response', 'company', 'state', 'zip_code',
            'tags', 'consumer_consent_provided', 'submitted_via',
            'date_sent_to_company', 'company_response_to_consumer',
            'timely_response', 'consumer_disputed'
        ]
        
        # SQL de inserção com ON CONFLICT (upsert)
        insert_sql = sql.SQL("""
            INSERT INTO raw.consumer_complaints ({})
            VALUES ({})
            ON CONFLICT (complaint_id) DO UPDATE SET
                date_received = EXCLUDED.date_received,
                product = EXCLUDED.product,
                sub_product = EXCLUDED.sub_product,
                issue = EXCLUDED.issue,
                sub_issue = EXCLUDED.sub_issue,
                consumer_complaint_narrative = EXCLUDED.consumer_complaint_narrative,
                company_public_response = EXCLUDED.company_public_response,
                company = EXCLUDED.company,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                tags = EXCLUDED.tags,
                consumer_consent_provided = EXCLUDED.consumer_consent_provided,
                submitted_via = EXCLUDED.submitted_via,
                date_sent_to_company = EXCLUDED.date_sent_to_company,
                company_response_to_consumer = EXCLUDED.company_response_to_consumer,
                timely_response = EXCLUDED.timely_response,
                consumer_disputed = EXCLUDED.consumer_disputed,
                loaded_at = CURRENT_TIMESTAMP
        """).format(
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        
        with conn.cursor() as cur:
            # Executar inserção em massa
            values = [[rec.get(col) for col in columns] for rec in records]
            extras.execute_batch(cur, insert_sql, values, page_size=1000)
            conn.commit()
        
        logger.info(f"  ✓ Batch {batch_num}: {len(df)} registros inseridos")
        
    except Exception as e:
        logger.error(f"  ✗ Erro ao inserir batch {batch_num}: {e}")
        conn.rollback()
        raise

def validate_data(conn):
    """
    Valida dados carregados
    """
    try:
        with conn.cursor() as cur:
            # Total de registros
            cur.execute("SELECT COUNT(*) FROM raw.consumer_complaints;")
            total = cur.fetchone()[0]
            
            # Registros com datas válidas
            cur.execute("""
                SELECT COUNT(*) 
                FROM raw.consumer_complaints 
                WHERE date_received IS NOT NULL;
            """)
            valid_dates = cur.fetchone()[0]
            
            # Produtos distintos
            cur.execute("""
                SELECT COUNT(DISTINCT product) 
                FROM raw.consumer_complaints;
            """)
            distinct_products = cur.fetchone()[0]
            
            logger.info("\n" + "="*60)
            logger.info("📊 VALIDAÇÃO DE DADOS")
            logger.info("="*60)
            logger.info(f"Total de registros: {total:,}")
            logger.info(f"Registros com data válida: {valid_dates:,} ({valid_dates/total*100:.1f}%)")
            logger.info(f"Produtos distintos: {distinct_products}")
            logger.info("="*60 + "\n")
            
    except Exception as e:
        logger.error(f"✗ Erro na validação: {e}")

# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================
def main():
    """
    Função principal de ingestão
    """
    start_time = datetime.now()
    logger.info("\n" + "="*60)
    logger.info("🚀 INICIANDO INGESTÃO CFPB")
    logger.info("="*60)
    logger.info(f"Data/Hora: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Arquivo: {Config.DATA_PATH}")
    logger.info(f"Destino: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
    logger.info(f"Schema: {Config.DB_SCHEMA}")
    logger.info(f"Tabela: {Config.DB_TABLE}")
    logger.info("="*60 + "\n")
    
    # Verificar se arquivo existe
    if not os.path.exists(Config.DATA_PATH):
        logger.error(f"✗ Arquivo não encontrado: {Config.DATA_PATH}")
        logger.info(f"💡 Dica: Baixe o dataset com:")
        logger.info(f"   curl -o {Config.DATA_PATH} 'https://files.consumerfinance.gov/ccdb/complaints.csv'")
        sys.exit(1)
    
    # Conectar ao banco
    conn = get_db_connection()
    
    try:
        # Criar tabela
        create_table(conn)
        
        # Truncar para Full Refresh
        truncate_table(conn)
        
        # Processar CSV em chunks
        logger.info("📥 Processando CSV em chunks...")
        total_records = 0
        batch_num = 0
        
        for chunk in pd.read_csv(
            Config.DATA_PATH,
            chunksize=Config.CHUNK_SIZE,
            low_memory=False,
            encoding='utf-8'
        ):
            batch_num += 1
            logger.info(f"\n📦 Processando batch {batch_num}...")
            
            # Limpar dados
            chunk = clean_dataframe(chunk)
            
            # Inserir no banco
            insert_batch(conn, chunk, batch_num)
            
            total_records += len(chunk)
            logger.info(f"  Total acumulado: {total_records:,} registros")
        
        # Validar dados
        validate_data(conn)
        
        # Finalizar
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("✅ INGESTÃO CONCLUÍDA COM SUCESSO!")
        logger.info("="*60)
        logger.info(f"Total de registros: {total_records:,}")
        logger.info(f"Total de batches: {batch_num}")
        logger.info(f"Tempo de execução: {duration:.2f} segundos ({duration/60:.2f} min)")
        logger.info(f"Taxa: {total_records/duration:.0f} registros/segundo")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"\n✗ ERRO NA INGESTÃO: {e}")
        sys.exit(1)
    
    finally:
        conn.close()
        logger.info("Conexão fechada")

if __name__ == "__main__":
    main()
