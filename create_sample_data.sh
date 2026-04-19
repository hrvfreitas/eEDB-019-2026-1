#!/bin/bash
#
# Cria dados de exemplo para teste rápido do pipeline
# Execução: ./create_sample_data.sh
#

echo "================================================"
echo "🔧 Criando Dados de Exemplo para Teste"
echo "================================================"
echo ""

docker exec -i projeto_postgres psql -U postgres -d projeto << 'EOF'

-- Cria 1000 registros de exemplo
INSERT INTO raw.consumer_complaints (
    complaint_id,
    date_received,
    product,
    sub_product,
    issue,
    sub_issue,
    company,
    state,
    zip_code,
    submitted_via,
    date_sent_to_company,
    company_response,
    timely_response,
    consumer_disputed,
    complaint_what_happened
)
SELECT 
    i as complaint_id,
    CURRENT_DATE - (random() * 365)::int as date_received,
    (ARRAY['Credit card', 'Mortgage', 'Student loan', 'Debt collection', 'Bank account or service'])[floor(random() * 5 + 1)] as product,
    'General' as sub_product,
    (ARRAY['Billing disputes', 'Loan modification', 'Incorrect information', 'Closing an account', 'Managing an account'])[floor(random() * 5 + 1)] as issue,
    NULL as sub_issue,
    (ARRAY['Bank of America', 'Wells Fargo', 'Chase', 'Citibank', 'Capital One', 'Discover', 'American Express'])[floor(random() * 7 + 1)] as company,
    (ARRAY['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'])[floor(random() * 10 + 1)] as state,
    lpad((random() * 99999)::int::text, 5, '0') as zip_code,
    (ARRAY['Web', 'Phone', 'Referral', 'Postal mail'])[floor(random() * 4 + 1)] as submitted_via,
    (CURRENT_DATE - (random() * 365)::int) + (random() * 7)::int as date_sent_to_company,
    (ARRAY['Closed with explanation', 'Closed with monetary relief', 'Closed with non-monetary relief', 'Closed'])[floor(random() * 4 + 1)] as company_response,
    (ARRAY['Yes', 'No'])[floor(random() * 2 + 1)] as timely_response,
    (ARRAY['Yes', 'No'])[floor(random() * 2 + 1)] as consumer_disputed,
    'Sample complaint data for testing purposes' as complaint_what_happened
FROM generate_series(1, 1000) as i
ON CONFLICT (complaint_id) DO NOTHING;

-- Mostra estatísticas
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT product) as unique_products,
    COUNT(DISTINCT company) as unique_companies,
    MIN(date_received) as oldest_complaint,
    MAX(date_received) as newest_complaint
FROM raw.consumer_complaints;

EOF

echo ""
echo "✅ Dados de exemplo criados!"
echo ""
echo "Próximo passo:"
echo "  docker-compose restart dbt"
echo ""
