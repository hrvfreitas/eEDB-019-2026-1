#!/bin/bash
#
# Download automático do dataset CFPB do Kaggle
# Dataset: https://www.kaggle.com/datasets/iuriivoloshyn/cfpb-consumer-complaint-database
# Requer: KAGGLE_USERNAME e KAGGLE_KEY configurados
#

set -e

echo "=========================================="
echo "📥 Download Dataset CFPB do Kaggle"
echo "=========================================="
echo ""

# Cores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Verifica se kaggle está instalado
if ! command -v kaggle &> /dev/null; then
    echo -e "${YELLOW}⚠️  Kaggle CLI não encontrado. Instalando...${NC}"
    pip install --user kaggle
    export PATH="$HOME/.local/bin:$PATH"
fi

# Verifica credenciais
if [ -z "$KAGGLE_USERNAME" ] || [ -z "$KAGGLE_KEY" ]; then
    echo -e "${RED}❌ Credenciais Kaggle não configuradas!${NC}"
    echo ""
    echo "Configure suas credenciais:"
    echo ""
    echo "export KAGGLE_USERNAME='hrvfreitas'"
    echo "export KAGGLE_KEY='<sua-chave-api>'"
    echo ""
    echo "Ou crie o arquivo ~/.kaggle/kaggle.json:"
    echo '{"username":"hrvfreitas","key":"<sua-chave-api>"}'
    echo ""
    exit 1
fi

# Cria diretório de dados se não existir
mkdir -p data

echo -e "${GREEN}✓${NC} Credenciais configuradas: $KAGGLE_USERNAME"
echo ""

# Baixa dataset
echo "📥 Baixando dataset do Kaggle..."
echo "   Dataset: iuriivoloshyn/cfpb-consumer-complaint-database"
echo ""

kaggle datasets download -d iuriivoloshyn/cfpb-consumer-complaint-database -p data/ --unzip

echo ""
echo -e "${GREEN}✓${NC} Download concluído!"
echo ""

# Verifica arquivo baixado
if [ -f "data/complaints.csv" ]; then
    FILE_SIZE=$(du -h data/complaints.csv | cut -f1)
    LINE_COUNT=$(wc -l < data/complaints.csv)
    echo -e "${GREEN}✓${NC} Arquivo: data/complaints.csv"
    echo -e "${GREEN}✓${NC} Tamanho: $FILE_SIZE"
    echo -e "${GREEN}✓${NC} Linhas: $LINE_COUNT"
    echo ""
    echo "=========================================="
    echo "✅ Pronto para ingestão!"
    echo "=========================================="
    echo ""
    echo "Próximo passo:"
    echo "  docker-compose run --rm ingestion python /scripts/ingest_cfpb.py"
    echo ""
else
    echo -e "${RED}❌ Erro: arquivo complaints.csv não encontrado${NC}"
    echo ""
    echo "Arquivos baixados:"
    ls -lh data/
    exit 1
fi
