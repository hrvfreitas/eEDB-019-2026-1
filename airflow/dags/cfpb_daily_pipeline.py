"""
DAG CFPB Complaints Pipeline - Versão com Script Python Customizado
Substitui Airbyte por script Python de ingestão customizado


Projeto: PECE Big Data - Fundamentos de Engenharia de Dados
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import logging

# ============================================================
# CONFIGURAÇÕES DA DAG
# ============================================================
default_args = {
    'owner': 'pece',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cfpb_complaints_daily_pipeline',
    default_args=default_args,
    description='Pipeline ELT completo CFPB com ingestão Python customizada',
    schedule_interval='0 3 * * *',  # 03:00 AM diário
    catchup=False,
    tags=['cfpb', 'elt', 'production', 'python-ingestion'],
)

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def check_data_quality(**context):
    """
    Callback para verificar qualidade após pipeline
    """
    import psycopg2
    import os
    
    logger = logging.getLogger(__name__)
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'projeto'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres')
        )
        
        with conn.cursor() as cur:
            # Verificar contagem de registros no gold
            cur.execute("SELECT COUNT(*) FROM gold.fact_complaints;")
            count = cur.fetchone()[0]
            
            logger.info(f"✅ Qualidade OK: {count:,} registros em gold.fact_complaints")
            
            if count < 100000:
                logger.warning(f"⚠️ ATENÇÃO: Apenas {count} registros (esperado >100k)")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"✗ Erro na verificação de qualidade: {e}")
        raise

# ============================================================
# TASKS DA DAG
# ============================================================

# TASK 1: Verificar se arquivo de dados existe
check_data_file = FileSensor(
    task_id='check_data_file_exists',
    filepath='/data/complaints.csv',
    poke_interval=30,
    timeout=300,
    mode='poke',
    dag=dag,
)

# TASK 2: Executar script Python de ingestão
run_python_ingestion = BashOperator(
    task_id='run_python_ingestion',
    bash_command='python /opt/airflow/scripts/ingest_cfpb.py',
    env={
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'projeto',
        'POSTGRES_USER': 'postgres',
        'POSTGRES_PASSWORD': 'postgres',
        'DATA_PATH': '/data/complaints.csv'
    },
    dag=dag,
)

# TASK 3: Executar Great Expectations
run_great_expectations = BashOperator(
    task_id='run_great_expectations',
    bash_command='cd /opt/airflow/great_expectations && great_expectations checkpoint run daily_validation',
    dag=dag,
)

# TASK 4: Executar dbt deps (garantir pacotes instalados)
run_dbt_deps = BashOperator(
    task_id='run_dbt_deps',
    bash_command='cd /opt/airflow/dbt && dbt deps --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

# TASK 5: Executar dbt run (staging + gold)
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

# TASK 6: Executar dbt test
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

# TASK 7: Verificar qualidade final
check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

# TASK 8: Gerar documentação dbt (opcional)
generate_dbt_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

# ============================================================
# DEPENDÊNCIAS (PIPELINE FLOW)
# ============================================================
check_data_file >> run_python_ingestion >> run_great_expectations >> run_dbt_deps >> run_dbt_models >> run_dbt_tests >> check_quality >> generate_dbt_docs
