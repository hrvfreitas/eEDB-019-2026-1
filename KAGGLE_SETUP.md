# 🔑 Como Configurar Credenciais do Kaggle

Para baixar o dataset CFPB do Kaggle automaticamente, você precisa configurar suas credenciais.

---

## 📋 **Passo a Passo**

### 1️⃣ **Obter sua API Key do Kaggle**

1. Acesse: https://www.kaggle.com/settings/account
2. Role até a seção **"API"**
3. Clique em **"Create New Token"**
4. Um arquivo `kaggle.json` será baixado com este formato:
   ```json
   {
     "username": "hrvfreitas",
     "key": "abc123def456..."
   }
   ```

### 2️⃣ **Configurar no Linux/Mac**

**Opção A - Arquivo de configuração (recomendado):**
```bash
# Crie o diretório
mkdir -p ~/.kaggle

# Copie o arquivo baixado
cp ~/Downloads/kaggle.json ~/.kaggle/

# Ajuste permissões
chmod 600 ~/.kaggle/kaggle.json
```

**Opção B - Variáveis de ambiente:**
```bash
export KAGGLE_USERNAME='hrvfreitas'
export KAGGLE_KEY='abc123def456...'
```

### 3️⃣ **Baixar o Dataset**

```bash
# Agora execute o script
./download_kaggle_data.sh
```

---

## 🪟 **Windows**

### PowerShell:
```powershell
# Criar diretório
mkdir $env:USERPROFILE\.kaggle

# Copiar kaggle.json para lá
copy Downloads\kaggle.json $env:USERPROFILE\.kaggle\

# Executar download
bash download_kaggle_data.sh
```

### CMD:
```cmd
mkdir %USERPROFILE%\.kaggle
copy Downloads\kaggle.json %USERPROFILE%\.kaggle\
bash download_kaggle_data.sh
```

---

## ✅ **Verificar Configuração**

```bash
# Teste se funciona
kaggle datasets list

# Se aparecer uma lista, está configurado corretamente!
```

---

## 🐳 **Usando dentro do Docker**

Se preferir baixar dentro do container:

```bash
# Entre no container
docker-compose run --rm -e KAGGLE_USERNAME=hrvfreitas -e KAGGLE_KEY=abc123... ingestion bash

# Dentro do container
pip install kaggle
kaggle datasets download -d iuriivoloshyn/cfpb-consumer-complaint-database --unzip
```

---

## 🔐 **Segurança**

⚠️ **IMPORTANTE:** Nunca commite `kaggle.json` ou suas credenciais no Git!

O arquivo `.gitignore` já está configurado para ignorar:
- `.kaggle/`
- `kaggle.json`
- `.env`

---

## 📚 **Referências**

- Kaggle API Docs: https://github.com/Kaggle/kaggle-api
- Dataset CFPB: https://www.kaggle.com/datasets/iuriivoloshyn/cfpb-consumer-complaint-database
- Sua conta: https://www.kaggle.com/hrvfreitas
