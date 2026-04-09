-- Script de Inicialização PostgreSQL
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Script de Inicialização PostgreSQL
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA raw IS 'Bronze Layer - Dados brutos do script Python';
COMMENT ON SCHEMA silver IS 'Silver Layer - Dados limpos (staging dbt)';
COMMENT ON SCHEMA gold IS 'Gold Layer - Star Schema para analytics';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

GRANT USAGE ON SCHEMA raw TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;

DO $$
BEGIN
    RAISE NOTICE 'Schemas criados: raw, silver, gold';
END $$;
COMMENT ON SCHEMA raw IS 'Bronze Layer - Dados brutos do script Python';
COMMENT ON SCHEMA silver IS 'Silver Layer - Dados limpos (staging dbt)';
COMMENT ON SCHEMA gold IS 'Gold Layer - Star Schema para analytics';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

GRANT USAGE ON SCHEMA raw TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO postgres;

DO $$
BEGIN
    RAISE NOTICE 'Schemas criados: raw, silver, gold';
END $$;
