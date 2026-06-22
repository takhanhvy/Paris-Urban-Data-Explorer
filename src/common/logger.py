"""
Logger structuré pour Urban Data Explorer.
Utilise structlog pour des logs JSON en production, lisibles en dev.
"""

import logging
import logging.config
import os
from pathlib import Path

import yaml


def setup_logging(config_path: str = "config/logging.yaml") -> None:
    """Configure le logging depuis le fichier YAML."""
    path = Path(config_path)
    if path.exists():
        with open(path) as f:
            config = yaml.safe_load(f)
        # Créer le dossier logs s'il n'existe pas
        Path("monitoring/logs").mkdir(parents=True, exist_ok=True)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """Retourne un logger nommé.

    Usage:
        logger = get_logger(__name__)
        logger.info("Ingestion DVF démarrée", extra={"source": "dvf", "annee": 2024})
    """
    return logging.getLogger(name)
