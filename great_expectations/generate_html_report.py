#!/usr/bin/env python3
"""
Gerador de Relatório HTML Customizado para Great Expectations
Baseado na apresentação do projeto eEDB-019-2026-1
"""

import json
from datetime import datetime
from pathlib import Path


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Great Expectations - Relatório de Qualidade CFPB</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
        }
        
        .header p {
            font-size: 1.1em;
            opacity: 0.9;
        }
        
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }
        
        .summary-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s;
        }
        
        .summary-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.2);
        }
        
        .summary-card .number {
            font-size: 3em;
            font-weight: bold;
            margin: 10px 0;
        }
        
        .summary-card .label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .success { color: #10b981; }
        .error { color: #ef4444; }
        .warning { color: #f59e0b; }
        .info { color: #3b82f6; }
        
        .expectations {
            padding: 40px;
        }
        
        .expectations h2 {
            font-size: 2em;
            margin-bottom: 30px;
            color: #1e3c72;
        }
        
        .expectation-card {
            background: white;
            border-left: 5px solid #ddd;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            transition: all 0.3s;
        }
        
        .expectation-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        
        .expectation-card.pass {
            border-left-color: #10b981;
            background: #f0fdf4;
        }
        
        .expectation-card.fail {
            border-left-color: #ef4444;
            background: #fef2f2;
        }
        
        .expectation-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        .expectation-title {
            font-size: 1.2em;
            font-weight: 600;
            color: #1e3c72;
        }
        
        .expectation-badge {
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.85em;
            text-transform: uppercase;
        }
        
        .badge-pass {
            background: #10b981;
            color: white;
        }
        
        .badge-fail {
            background: #ef4444;
            color: white;
        }
        
        .expectation-description {
            color: #666;
            margin: 10px 0;
            line-height: 1.6;
        }
        
        .expectation-details {
            background: rgba(0,0,0,0.02);
            padding: 15px;
            border-radius: 8px;
            margin-top: 15px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }
        
        .footer {
            background: #1e3c72;
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .footer p {
            margin: 5px 0;
            opacity: 0.9;
        }
        
        .metadata {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-top: 20px;
            padding: 20px;
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
        }
        
        .metadata-item {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .metadata-label {
            font-weight: 600;
            opacity: 0.8;
        }
        
        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8em;
            }
            
            .summary {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Great Expectations</h1>
            <p>Relatório de Qualidade de Dados - CFPB Consumer Complaints</p>
            <p style="font-size: 0.9em; margin-top: 10px; opacity: 0.8;">
                Projeto eEDB-019-2026-1 | Grupo 4
            </p>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <div class="label">Total de Validações</div>
                <div class="number info">{total_expectations}</div>
            </div>
            <div class="summary-card">
                <div class="label">✓ Aprovadas</div>
                <div class="number success">{successful_expectations}</div>
            </div>
            <div class="summary-card">
                <div class="label">✗ Falharam</div>
                <div class="number error">{failed_expectations}</div>
            </div>
            <div class="summary-card">
                <div class="label">Taxa de Sucesso</div>
                <div class="number {success_class}">{success_rate}%</div>
            </div>
        </div>
        
        <div class="expectations">
            <h2>📋 Validações Executadas (E1-E8)</h2>
            {expectations_html}
        </div>
        
        <div class="footer">
            <p><strong>Execução:</strong> {execution_time}</p>
            <div class="metadata">
                <div class="metadata-item">
                    <span class="metadata-label">Datasource:</span>
                    <span>PostgreSQL - schema raw</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Tabela:</span>
                    <span>consumer_complaints</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Checkpoint:</span>
                    <span>cfpb_quality_checkpoint</span>
                </div>
                <div class="metadata-item">
                    <span class="metadata-label">Autores:</span>
                    <span>Daniely · Diane · Hercules</span>
                </div>
            </div>
            <p style="margin-top: 20px; font-size: 0.9em;">
                github.com/hrvfreitas/eEDB-019-2026-1 | Abril 2026
            </p>
        </div>
    </div>
</body>
</html>
"""


def generate_html_report(validation_results, output_path="great_expectations_report.html"):
    """
    Gera relatório HTML customizado a partir dos resultados do Great Expectations
    """
    
    # Processa resultados
    total = len(validation_results)
    successful = sum(1 for r in validation_results if r.get('success', False))
    failed = total - successful
    success_rate = round((successful / total * 100) if total > 0 else 0, 1)
    
    # Determina classe CSS para taxa de sucesso
    if success_rate >= 95:
        success_class = "success"
    elif success_rate >= 80:
        success_class = "warning"
    else:
        success_class = "error"
    
    # Gera HTML das expectativas
    expectations_html = []
    for i, result in enumerate(validation_results, 1):
        success = result.get('success', False)
        expectation_type = result.get('expectation_config', {}).get('expectation_type', 'Unknown')
        column = result.get('expectation_config', {}).get('kwargs', {}).get('column', 'N/A')
        description = result.get('expectation_config', {}).get('meta', {}).get('description', '')
        
        # Extrai detalhes do resultado
        observed = result.get('result', {}).get('observed_value', 'N/A')
        unexpected_count = result.get('result', {}).get('unexpected_count', 0)
        
        card_class = "pass" if success else "fail"
        badge_class = "badge-pass" if success else "badge-fail"
        badge_text = "✓ PASSOU" if success else "✗ FALHOU"
        
        exp_html = f"""
        <div class="expectation-card {card_class}">
            <div class="expectation-header">
                <div class="expectation-title">E{i}: {column}</div>
                <div class="expectation-badge {badge_class}">{badge_text}</div>
            </div>
            <div class="expectation-description">
                {description}
            </div>
            <div class="expectation-details">
                <strong>Tipo:</strong> {expectation_type}<br>
                <strong>Valor Observado:</strong> {observed}<br>
                {f'<strong>Registros Inesperados:</strong> {unexpected_count}<br>' if unexpected_count > 0 else ''}
            </div>
        </div>
        """
        expectations_html.append(exp_html)
    
    # Monta HTML final
    html = HTML_TEMPLATE.format(
        total_expectations=total,
        successful_expectations=successful,
        failed_expectations=failed,
        success_rate=success_rate,
        success_class=success_class,
        expectations_html='\n'.join(expectations_html),
        execution_time=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    )
    
    # Salva arquivo
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✓ Relatório HTML gerado: {output_path}")
    print(f"  Total: {total} | Sucesso: {successful} | Falha: {failed} | Taxa: {success_rate}%")
    
    return output_path


def create_sample_report():
    """
    Cria relatório de exemplo para demonstração
    """
    sample_results = [
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "complaint_id"},
                "meta": {"description": "E1 - Chave primária - sem ID o registro não é rastreável"}
            },
            "result": {"observed_value": "100%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "complaint_id"},
                "meta": {"description": "E2 - Garante que não há duplicatas na ingestão"}
            },
            "result": {"observed_value": "100%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "product"},
                "meta": {"description": "E3 - Produto fora do conjunto indica dado corrompido"}
            },
            "result": {"observed_value": "98.5%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "date_received"},
                "meta": {"description": "E4 - Datas malformadas quebram o cálculo de response_days"}
            },
            "result": {"observed_value": "DATE"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_value_lengths_to_equal",
                "kwargs": {"column": "state"},
                "meta": {"description": "E5 - Sigla americana - outros valores indicam erro de entrada"}
            },
            "result": {"observed_value": "99.2%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "timely_response"},
                "meta": {"description": "E6 - Campo booleano textual - outros valores corrompem o KPI"}
            },
            "result": {"observed_value": "100%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "submitted_via"},
                "meta": {"description": "E7 - Garante Web, Telefone, Carta, Fax ou Referência"}
            },
            "result": {"observed_value": "99.8%"}
        },
        {
            "success": True,
            "expectation_config": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "company"},
                "meta": {"description": "E8 - Sem empresa não é possível responder às perguntas de negócio"}
            },
            "result": {"observed_value": "100%"}
        }
    ]
    
    return generate_html_report(sample_results, "great_expectations_report_sample.html")


if __name__ == "__main__":
    print("Gerando relatório de exemplo...")
    report_path = create_sample_report()
    print(f"\n✓ Relatório disponível em: {report_path}")
    print("  Abra no navegador para visualizar!")
