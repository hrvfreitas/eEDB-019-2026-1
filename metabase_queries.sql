-- ============================================================
-- CONSULTAS METABASE - CFPB Complaints Dashboard
-- 4 consultas principais conforme apresentação do projeto
-- ============================================================

-- ============================================================
-- CONSULTA A: Quais produtos têm mais reclamações?
-- Gráfico de barras - Top 10 produtos por volume
-- ============================================================
SELECT 
    p.product_name,
    p.sub_product,
    COUNT(*) as total_complaints,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM gold.fact_complaints f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.sub_product
ORDER BY total_complaints DESC
LIMIT 10;

-- ============================================================
-- CONSULTA B: Quais empresas acumulam mais reclamações?
-- Ranking horizontal - acumulado total e por período
-- ============================================================
SELECT 
    c.company_name,
    COUNT(*) as total_complaints,
    COUNT(CASE WHEN d.year = EXTRACT(YEAR FROM CURRENT_DATE) THEN 1 END) as complaints_this_year,
    COUNT(CASE WHEN d.year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN 1 END) as complaints_last_year,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as market_share_complaints
FROM gold.fact_complaints f
JOIN gold.dim_company c ON f.company_key = c.company_key
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY c.company_name
ORDER BY total_complaints DESC
LIMIT 10;

-- ============================================================
-- CONSULTA C: Qual empresa tem a pior taxa de resposta?
-- Tabela rankeada - % respostas fora do prazo
-- ============================================================
SELECT 
    c.company_name,
    COUNT(*) as total_complaints,
    SUM(CASE WHEN f.is_timely = FALSE THEN 1 ELSE 0 END) as late_responses,
    SUM(CASE WHEN f.is_timely = TRUE THEN 1 ELSE 0 END) as timely_responses,
    ROUND(
        SUM(CASE WHEN f.is_timely = FALSE THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) as late_response_rate,
    ROUND(AVG(f.response_days), 1) as avg_response_days
FROM gold.fact_complaints f
JOIN gold.dim_company c ON f.company_key = c.company_key
WHERE f.response_days IS NOT NULL
GROUP BY c.company_name
HAVING COUNT(*) >= 100  -- Apenas empresas com volume significativo
ORDER BY late_response_rate DESC
LIMIT 20;

-- ============================================================
-- CONSULTA D: Issues por região do país?
-- Heatmap estado × categoria de issue
-- ============================================================
SELECT 
    g.state,
    g.region,
    p.product_name,
    p.issue,
    COUNT(*) as complaint_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY g.region), 2) as pct_in_region
FROM gold.fact_complaints f
JOIN gold.dim_geography g ON f.geo_key = g.geo_key
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE g.state IS NOT NULL
GROUP BY g.state, g.region, p.product_name, p.issue
ORDER BY g.region, complaint_count DESC;

-- ============================================================
-- CONSULTA EXTRA 1: Timeline de reclamações
-- Evolução temporal por produto
-- ============================================================
SELECT 
    d.year,
    d.quarter_name,
    d.month_name,
    p.product_name,
    COUNT(*) as complaints,
    AVG(f.response_days) as avg_response_days,
    SUM(CASE WHEN f.is_timely = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as timely_pct
FROM gold.fact_complaints f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE d.year >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
GROUP BY d.year, d.quarter_name, d.month_name, d.month_num, p.product_name
ORDER BY d.year DESC, d.month_num DESC, complaints DESC;

-- ============================================================
-- CONSULTA EXTRA 2: Análise de disputas
-- Produtos e empresas com maior taxa de disputa do consumidor
-- ============================================================
SELECT 
    p.product_name,
    c.company_name,
    COUNT(*) as total_complaints,
    SUM(CASE WHEN f.is_disputed = TRUE THEN 1 ELSE 0 END) as disputed_count,
    ROUND(
        SUM(CASE WHEN f.is_disputed = TRUE THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) as dispute_rate
FROM gold.fact_complaints f
JOIN gold.dim_product p ON f.product_key = p.product_key
JOIN gold.dim_company c ON f.company_key = c.company_key
GROUP BY p.product_name, c.company_name
HAVING COUNT(*) >= 50
ORDER BY dispute_rate DESC, total_complaints DESC
LIMIT 30;
