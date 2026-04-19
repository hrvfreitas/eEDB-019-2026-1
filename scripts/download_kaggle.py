#!/usr/bin/env python3
"""
Download do dataset CFPB via Kaggle API
Dataset: kaggle/us-consumer-finance-complaints

Requer variáveis de ambiente:
  KAGGLE_USERNAME  - usuário Kaggle
  KAGGLE_KEY       - token da API Kaggle
"""

import os
import sys
import json
import zipfile
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DATASET = "kaggle/us-consumer-finance-complaints"
DATA_DIR = Path(os.getenv("DATA_PATH", "/data/complaints.csv")).parent
OUTPUT_CSV = DATA_DIR / "complaints.csv"


def setup_kaggle_credentials():
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")
    if not username or not key:
        logger.error("Variáveis KAGGLE_USERNAME e KAGGLE_KEY são obrigatórias.")
        sys.exit(1)

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"
    kaggle_json.write_text(json.dumps({"username": username, "key": key}))
    kaggle_json.chmod(0o600)
    logger.info(f"Credenciais Kaggle configuradas para: {username}")


def download_dataset():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Baixando dataset: {DATASET}")
    result = subprocess.run(
        ["kaggle", "datasets", "download", "-d", DATASET, "-p", str(DATA_DIR)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Erro no download:\n{result.stderr}")
        sys.exit(1)

    logger.info(result.stdout.strip())

    zip_files = list(DATA_DIR.glob("*.zip"))
    if not zip_files:
        logger.error("Nenhum arquivo ZIP encontrado após download.")
        sys.exit(1)

    zip_path = zip_files[0]
    logger.info(f"Extraindo {zip_path.name}...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        logger.info(f"Arquivos no ZIP: {members}")

        csv_members = [m for m in members if m.endswith(".csv")]
        if not csv_members:
            logger.error("Nenhum CSV encontrado dentro do ZIP.")
            sys.exit(1)

        target = max(csv_members, key=lambda m: zf.getinfo(m).file_size)
        logger.info(f"Extraindo: {target}")
        zf.extract(target, path=str(DATA_DIR))

        extracted = DATA_DIR / target
        if extracted != OUTPUT_CSV:
            extracted.rename(OUTPUT_CSV)

    zip_path.unlink()
    logger.info(f"Dataset salvo em: {OUTPUT_CSV}")
    logger.info(f"Tamanho: {OUTPUT_CSV.stat().st_size / 1024 / 1024:.1f} MB")


def main():
    if OUTPUT_CSV.exists():
        logger.info(f"Dataset já existe em {OUTPUT_CSV}, pulando download.")
        return

    setup_kaggle_credentials()
    download_dataset()
    logger.info("Download concluido com sucesso.")


if __name__ == "__main__":
    main()
