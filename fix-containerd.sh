#!/bin/bash
#
# Script de Correção - Erro containerd
# Resolve: "failed to create prepare snapshot dir"
#

set -e

echo "=========================================="
echo "🔧 Correção de Erro containerd"
echo "=========================================="
echo ""

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn "Detectado erro: 'failed to create prepare snapshot dir'"
echo ""
log_info "Este erro ocorre quando o containerd está corrompido."
echo ""

# Verifica se é Ubuntu/Debian
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS="unknown"
fi

log_info "Sistema operacional: $OS"
echo ""

# Para todos os containers
log_info "1/7 Parando todos os containers..."
docker stop $(docker ps -aq) 2>/dev/null || true
echo ""

# Para o Docker
log_info "2/7 Parando serviço Docker..."
if command -v systemctl &> /dev/null; then
    sudo systemctl stop docker
    sudo systemctl stop containerd
else
    log_warn "systemctl não encontrado, tentando parar manualmente..."
    sudo service docker stop 2>/dev/null || true
fi
echo ""

# Remove diretório corrompido do containerd
log_info "3/7 Removendo diretórios corrompidos do containerd..."
sudo rm -rf /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
sudo rm -rf /var/lib/containerd/tmpmounts
sudo rm -rf /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/metadata.db
echo ""

# Recria estrutura
log_info "4/7 Recriando estrutura de diretórios..."
sudo mkdir -p /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
sudo mkdir -p /var/lib/containerd/tmpmounts
sudo chmod 700 /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
echo ""

# Reinicia containerd
log_info "5/7 Reiniciando containerd..."
if command -v systemctl &> /dev/null; then
    sudo systemctl restart containerd
    sudo systemctl restart docker
else
    sudo service containerd restart 2>/dev/null || true
    sudo service docker restart 2>/dev/null || true
fi
echo ""

# Aguarda Docker ficar pronto
log_info "6/7 Aguardando Docker ficar pronto..."
for i in {1..30}; do
    if docker info > /dev/null 2>&1; then
        log_info "Docker pronto! ✓"
        break
    fi
    echo -n "."
    sleep 2
done
echo ""

# Limpa cache
log_info "7/7 Limpando cache Docker..."
docker system prune -f
echo ""

echo "=========================================="
log_info "✅ Correção concluída!"
echo "=========================================="
echo ""
log_info "Agora execute:"
echo "  docker-compose up -d"
echo ""
log_warn "Se o erro persistir, execute:"
echo "  sudo systemctl restart docker"
echo "  docker-compose pull"
echo "  docker-compose up -d"
echo ""
