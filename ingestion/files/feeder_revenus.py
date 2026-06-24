"""
Feeder Revenus — XLSX INSEE FiLoSoFi 2018 vers data/raw/

Lit BASE_TD_FILO_DEC_IRIS_2018.xlsx (feuille IRIS_DEC, header ligne 6),
filtre les 870 IRIS parisiens (code IRIS commencant par '75'),
sauvegarde en CSV dans data/raw/ pour lecture directe par Spark feeder.py.

Spark ne peut pas lire XLSX nativement (pas de spark-excel Scala 2.13).

12 395 lignes France entiere -> 870 lignes Paris apres filtrage.

Usage :
    python -m ingestion.files.feeder_revenus
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

XLSX_FILENAME = "BASE_TD_FILO_DEC_IRIS_2018.xlsx"
SHEET_NAME = "IRIS_DEC"
HEADER_ROW = 5  # header en ligne 6 Excel = skiprows=5


@log_pipeline_run("feeder_revenus", "revenus", "raw")
def ingest_revenus(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """Lit le XLSX FiLoSoFi, filtre Paris, ecrit CSV dans data/raw/ pour Spark."""
    raw_dir = Path(raw_path or settings.data_raw_path)

    src = raw_dir / XLSX_FILENAME
    if not src.exists():
        raise IngestionError(f"Fichier introuvable : {src}")

    logger.info(f"Lecture : {src} (feuille={SHEET_NAME}, header ligne {HEADER_ROW + 1})")
    df_raw = pd.read_excel(
        src,
        sheet_name=SHEET_NAME,
        skiprows=HEADER_ROW,
        header=0,
        dtype=str,
    )
    # Nettoyer les noms de colonnes
    df_raw.columns = [c.strip() for c in df_raw.columns]

    nb_raw = len(df_raw)
    logger.info(f"  France entiere : {nb_raw:,} IRIS")

    # Filtrage Paris : code IRIS commence par '75'
    if "IRIS" not in df_raw.columns:
        raise IngestionError(f"Colonne IRIS introuvable parmi : {list(df_raw.columns)}")

    df_paris = df_raw[df_raw["IRIS"].str.startswith("75", na=False)].copy()
    nb_paris = len(df_paris)
    logger.info(f"  Paris (IRIS 75xxx) : {nb_paris:,} IRIS")

    if nb_paris == 0:
        raise IngestionError("Aucun IRIS Paris trouve — verifier le fichier source")

    # Sauvegarde dans data/raw/ -- Spark feeder.py lira ce CSV directement
    dst = raw_dir / "revenus_iris_paris.csv"
    df_paris.to_csv(dst, index=False, encoding="utf-8")
    logger.info(f"Revenus raw CSV : {nb_paris:,} IRIS Paris -> {dst}")

    return {"nb_france": nb_raw, "nb_paris": nb_paris}


if __name__ == "__main__":
    ingest_revenus()
