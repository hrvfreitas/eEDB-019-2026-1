#!/usr/bin/env python3
"""
Download the CFPB dataset from Kaggle directly into the pipeline data directory.
Prepared to run in Docker containers and orchestrated workflows such as Airflow.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import kagglehub

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/tmp/download_kaggle.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class KaggleDatasetDownloader:
    """
    Downloads a Kaggle dataset into a local output directory using kagglehub.
    """

    def __init__(self, dataset_slug: str, output_dir: str) -> None:
        self.dataset_slug = dataset_slug
        self.output_dir = Path(output_dir)

    def load_environment(self) -> None:
        """
        Validate that the Kaggle token is available in the environment.
        """
        kaggle_token = os.getenv("KAGGLE_API_TOKEN")

        if not kaggle_token:
            raise EnvironmentError(
                "KAGGLE_API_TOKEN not found. Please configure the Kaggle token."
            )

        logger.info("Kaggle token found successfully.")

    def prepare_output_directory(self) -> None:
        """
        Ensure the output directory exists.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Output directory prepared: %s", self.output_dir.resolve())

    def download_dataset(self) -> None:
        """
        Download the dataset directly into the output directory.
        """
        logger.info("Starting download for dataset: %s", self.dataset_slug)

        downloaded_path = kagglehub.dataset_download(
            self.dataset_slug,
            output_dir=str(self.output_dir),
            force_download=True,
        )

        logger.info("Dataset download completed successfully.")
        logger.info("Downloaded path: %s", downloaded_path)
        logger.info("Pipeline data directory: %s", self.output_dir.resolve())

    def run(self) -> None:
        """
        Execute the full download flow.
        """
        self.load_environment()
        self.prepare_output_directory()
        self.download_dataset()


if __name__ == "__main__":
    dataset_slug = os.getenv(
        "KAGGLE_DATASET_SLUG",
        "adamkelbrick/cfpb-consumer-complaint-database",
    )
    output_dir = os.getenv("DATA_PATH_DIR", "/data")

    downloader = KaggleDatasetDownloader(
        dataset_slug=dataset_slug,
        output_dir=output_dir,
    )
    downloader.run()