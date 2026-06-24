"""
Feeder DVF — CSV transactions immobilieres vers data/bronze/

Lit les fichiers dvf_75_YYYY.csv (separateur |) depuis data/raw/,
valide les colonnes essentielles, sauvegarde en Parquet partitionne par annee
dans data/bronze/dvf/ (format plus compact et plus rapide pour Spark).

Source : DVF Demandes de Valeurs Foncieres 2020-2025 (plusieurs millions de lignes).
Le filtrage Paris est deja fait par le nommage des fichiers (dvf_75_*).

Usage :
    python -m ingestion.files.feeder_dvf
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

# Colonnes minimales attendues dans les fichiers DVF
REQUIRED_COLS = {
    "date_mutation",
    "valeur_fonciere",
    "code_postal",
    "type_local",
    "surface_reelle_bati",
}


@log_pipeline_run("feeder_dvf", "dvf", "bronze")
def ingest_dvf(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """Lit tous les CSV DVF Paris, valide le schema, ecrit en Parquet bronze."""
    raw_dir = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path) / "dvf"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(raw_dir.glob("dvf_75_*.csv"))
    if not csv_files:
        raise IngestionError(f"Aucun fichier dvf_75_*.csv trouve dans {raw_dir}")

    total_rows = 0
    stats: dict[str, int] = {}

    for csv_file in csv_files:
        annee = csv_file.stem.split("_")[-1]  # dvf_75_2023.csv -> "2023"
        logger.info(f"Lecture : {csv_file.name}")

        df = pd.read_csv(
            csv_file,
            sep="|",
            encoding="utf-8",
            low_memory=False,
            dtype=str,  # tout en string — cast dans processor Spark
        )

        # Validation colonnes minimales
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            logger.warning(f"  Colonnes manquantes dans {csv_file.name} : {missing}")

        nb = len(df)
        stats[annee] = nb
        total_rows += nb
        logger.info(f"  {annee} : {nb:,} transactions")

        # Ecriture Parquet par annee (partitionnement simple pour Spark)
        dst = bronze_dir / f"dvf_75_{annee}.parquet"
        df.to_parquet(dst, index=False, engine="pyarrow")
        logger.info(f"  -> {dst}")

    logger.info(f"DVF Bronze : {total_rows:,} lignes totales — {len(csv_files)} fichiers")
    return {"nb_lignes_total": total_rows, "par_annee": stats}


if __name__ == "__main__":
    ingest_dvf()
