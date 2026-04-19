## 🚨 CORREÇÃO DE ERRO CONTAINERD

Se você ver este erro:
```
ERROR: failed to prepare extraction snapshot ... 
failed to create prepare snapshot dir: ... 
no such file or directory
```

**Execute:**

```bash
# Opção 1: Script automático (RECOMENDADO)
chmod +x fix-containerd.sh
./fix-containerd.sh

# Opção 2: Manual (se o script não funcionar)
sudo systemctl stop docker
sudo systemctl stop containerd
sudo rm -rf /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
sudo mkdir -p /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
sudo chmod 700 /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
sudo systemctl restart containerd
sudo systemctl restart docker
docker system prune -f

# Depois:
docker-compose pull
docker-compose up -d
```

**Causa:** Diretório do containerd corrompido ou removido.

**Solução:** Recria a estrutura de diretórios necessária.

---
