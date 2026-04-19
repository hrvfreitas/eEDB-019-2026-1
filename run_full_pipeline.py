#!/usr/bin/env python3
"""
Pipeline Completo CFPB - eEDB-019-2026-1
Integra Great Expectations (8 validações) + Relatório HTML + Consultas Metabase
Execução: python3 run_full_pipeline.py
"""

import os
import sys
import time
import psycopg2
from datetime import datetime
from pathlib import Path

# Configurações
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'projeto'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

class CFPBPipeline:
    def __init__(self):
        self.start_time = datetime.now()
        self.results = {
            'ingestion': False,
            'great_expectations': False,
            'dbt_run': False,
            'metabase_queries': False,
            'html_report': False
        }
    
    def log(self, message, level="INFO"):
        """Log formatado com timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        symbols = {
            "INFO": "ℹ️",
            "SUCCESS": "✅",
            "ERROR": "❌",
            "WARNING": "⚠️",
            "STEP": "🔹"
        }
        symbol = symbols.get(level, "  ")
        print(f"[{timestamp}] {symbol} {message}")
    
    def check_postgres(self):
        """Verifica se PostgreSQL está acessível"""
        self.log("Verificando conexão com PostgreSQL...", "STEP")
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            self.log(f"PostgreSQL conectado: {version[:50]}...", "SUCCESS")
            return True
        except Exception as e:
            self.log(f"Erro ao conectar PostgreSQL: {e}", "ERROR")
            return False
    
    def run_ingestion(self):
        """Passo 1: Ingestão de dados"""
        self.log("=" * 60, "INFO")
        self.log("PASSO 1: INGESTÃO DE DADOS (RAW)", "STEP")
        self.log("=" * 60, "INFO")
        
        try:
            # Verifica se tabela raw existe
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'raw' 
                AND table_name = 'consumer_complaints'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM raw.consumer_complaints")
                count = cursor.fetchone()[0]
                self.log(f"Tabela raw.consumer_complaints já existe: {count:,} registros", "SUCCESS")
                self.results['ingestion'] = True
            else:
                self.log("Tabela raw não encontrada. Execute a ingestão manualmente.", "WARNING")
                self.log("docker-compose run --rm ingestion python /scripts/ingest_cfpb.py", "INFO")
                self.results['ingestion'] = False
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            self.log(f"Erro na ingestão: {e}", "ERROR")
            self.results['ingestion'] = False
        
        return self.results['ingestion']
    
    def run_great_expectations(self):
        """Passo 2: Validações Great Expectations (8 modelos)"""
        self.log("=" * 60, "INFO")
        self.log("PASSO 2: GREAT EXPECTATIONS (8 VALIDAÇÕES)", "STEP")
        self.log("=" * 60, "INFO")
        
        try:
            # Importa Great Expectations
            from great_expectations.data_context import DataContext
            
            self.log("Carregando Great Expectations...", "INFO")
            context = DataContext(context_root_dir="./great_expectations")
            
            self.log("Executando checkpoint...", "INFO")
            checkpoint_result = context.run_checkpoint(
                checkpoint_name="cfpb_quality_checkpoint"
            )
            
            success = checkpoint_result.success
            self.results['great_expectations'] = success
            
            if success:
                self.log("Todas as 8 validações passaram!", "SUCCESS")
            else:
                self.log("Algumas validações falharam. Veja detalhes no relatório.", "WARNING")
            
            # Gera relatório HTML customizado
            self.log("Gerando relatório HTML...", "INFO")
            self.generate_html_report(checkpoint_result)
            
            return success
            
        except Exception as e:
            self.log(f"Erro no Great Expectations: {e}", "ERROR")
            self.results['great_expectations'] = False
            return False
    
    def generate_html_report(self, checkpoint_result):
        """Gera relatório HTML customizado"""
        try:
            import sys
            sys.path.append('./great_expectations')
            from generate_html_report import generate_html_report
            
            # Extrai resultados das validações
            validation_results = []
            for validation in checkpoint_result.run_results.values():
                for result in validation.results:
                    validation_results.append({
                        'success': result.success,
                        'expectation_config': result.expectation_config.to_json_dict(),
                        'result': result.result
                    })
            
            # Gera relatório
            output_path = "great_expectations_report.html"
            generate_html_report(validation_results, output_path)
            
            self.log(f"Relatório HTML: {output_path}", "SUCCESS")
            self.results['html_report'] = True
            
        except Exception as e:
            self.log(f"Erro ao gerar relatório HTML: {e}", "WARNING")
            self.results['html_report'] = False
    
    def run_dbt(self):
        """Passo 3: Transformações dbt (Silver + Gold)"""
        self.log("=" * 60, "INFO")
        self.log("PASSO 3: TRANSFORMAÇÕES DBT (SILVER + GOLD)", "STEP")
        self.log("=" * 60, "INFO")
        
        try:
            import subprocess
            
            self.log("Executando dbt run...", "INFO")
            result = subprocess.run(
                ["dbt", "run"],
                cwd="./dbt",
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.log("dbt run concluído com sucesso!", "SUCCESS")
                self.results['dbt_run'] = True
                
                # Mostra sumário
                for line in result.stdout.split('\n'):
                    if 'Completed' in line or 'Done' in line:
                        self.log(line.strip(), "INFO")
                
                return True
            else:
                self.log("dbt run falhou", "ERROR")
                self.log(result.stderr, "ERROR")
                self.results['dbt_run'] = False
                return False
                
        except Exception as e:
            self.log(f"Erro ao executar dbt: {e}", "ERROR")
            self.results['dbt_run'] = False
            return False
    
    def execute_metabase_queries(self):
        """Passo 4: Executa consultas do Metabase"""
        self.log("=" * 60, "INFO")
        self.log("PASSO 4: CONSULTAS METABASE (4 DASHBOARDS)", "STEP")
        self.log("=" * 60, "INFO")
        
        queries = {
            "A - Top 10 Produtos": """
                SELECT p.product_name, COUNT(*) as total
                FROM gold.fact_complaints f
                JOIN gold.dim_product p ON f.product_key = p.product_key
                GROUP BY p.product_name
                ORDER BY total DESC
                LIMIT 10
            """,
            "B - Top 10 Empresas": """
                SELECT c.company_name, COUNT(*) as total
                FROM gold.fact_complaints f
                JOIN gold.dim_company c ON f.company_key = c.company_key
                GROUP BY c.company_name
                ORDER BY total DESC
                LIMIT 10
            """,
            "C - Pior Taxa de Resposta": """
                SELECT c.company_name,
                    ROUND(SUM(CASE WHEN NOT f.is_timely THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as late_pct
                FROM gold.fact_complaints f
                JOIN gold.dim_company c ON f.company_key = c.company_key
                GROUP BY c.company_name
                HAVING COUNT(*) >= 100
                ORDER BY late_pct DESC
                LIMIT 10
            """,
            "D - Issues por Região": """
                SELECT g.region, p.product_name, COUNT(*) as total
                FROM gold.fact_complaints f
                JOIN gold.dim_geography g ON f.geo_key = g.geo_key
                JOIN gold.dim_product p ON f.product_key = p.product_key
                GROUP BY g.region, p.product_name
                ORDER BY total DESC
                LIMIT 20
            """
        }
        
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            results_summary = []
            
            for name, query in queries.items():
                self.log(f"Executando: {name}", "INFO")
                cursor.execute(query)
                rows = cursor.fetchall()
                self.log(f"  → {len(rows)} resultados", "SUCCESS")
                results_summary.append(f"{name}: {len(rows)} registros")
            
            cursor.close()
            conn.close()
            
            self.results['metabase_queries'] = True
            self.log("Todas as consultas executadas!", "SUCCESS")
            
            return True
            
        except Exception as e:
            self.log(f"Erro ao executar consultas: {e}", "ERROR")
            self.results['metabase_queries'] = False
            return False
    
    def print_summary(self):
        """Imprime sumário final"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        self.log("=" * 60, "INFO")
        self.log("SUMÁRIO FINAL", "STEP")
        self.log("=" * 60, "INFO")
        
        for step, success in self.results.items():
            status = "✅ SUCESSO" if success else "❌ FALHOU"
            self.log(f"{step.upper():<30} {status}", "INFO")
        
        total = len(self.results)
        passed = sum(self.results.values())
        
        self.log("=" * 60, "INFO")
        self.log(f"Tempo total: {elapsed:.1f}s", "INFO")
        self.log(f"Passos concluídos: {passed}/{total}", "INFO")
        
        if passed == total:
            self.log("🎉 PIPELINE COMPLETO EXECUTADO COM SUCESSO!", "SUCCESS")
        else:
            self.log("⚠️  Pipeline executado com falhas parciais", "WARNING")
    
    def run(self):
        """Executa pipeline completo"""
        self.log("🚀 INICIANDO PIPELINE CFPB COMPLETO", "STEP")
        self.log(f"Projeto: eEDB-019-2026-1 | Grupo 4", "INFO")
        self.log("=" * 60, "INFO")
        
        # Verifica PostgreSQL
        if not self.check_postgres():
            self.log("PostgreSQL não está acessível. Abortando.", "ERROR")
            return False
        
        # Executa passos
        self.run_ingestion()
        
        if self.results['ingestion']:
            self.run_great_expectations()
            self.run_dbt()
            self.execute_metabase_queries()
        else:
            self.log("Ingestão não concluída. Pulando passos seguintes.", "WARNING")
        
        # Sumário final
        self.print_summary()
        
        return all(self.results.values())


if __name__ == "__main__":
    pipeline = CFPBPipeline()
    success = pipeline.run()
    sys.exit(0 if success else 1)
