"""
Feeder Délinquance — Parquet INSEE vers data/bronze/

Lit le fichier Parquet national, filtre sur Paris (codes INSEE 75101–75120),
et sauvegarde dans data/bronze/delinquance_raw.parquet.
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

PARIS_CODES = [f"7510{i}" if i < 10 else f"751{i}" for i in range(1, 21)]
CODGEO_COL = "CODGEO_2025"  # colonne du code commune dans le fichier source


@log_pipeline_run("feeder_delinquance", "delinquance", "bronze")
def ingest_delinquance(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """
    Lit le Parquet délinquance national, filtre Paris, écrit en Bronze.
    """
    raw_dir = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # Cherche le fichier Parquet dans data/raw/
    parquet_files = list(raw_dir.glob("*.parquet"))
    if not parquet_files:
        raise IngestionError(f"Aucun fichier Parquet trouvé dans {raw_dir}")

    # Prend le premier Parquet trouvé (délinquance)
    source_file = parquet_files[0]
    logger.info(f"Lecture de {source_file.name}")

    df = pd.read_parquet(source_file)
    total_in = len(df)
    logger.info(f"  Total national : {total_in:,} lignes")

    # Filtre Paris
    df[CODGEO_COL] = df[CODGEO_COL].astype(str).str.strip()
    df_paris = df[df[CODGEO_COL].isin(PARIS_CODES)].copy()
    total_out = len(df_paris)

    output_file = bronze_dir / "delinquance_raw.parquet"
    df_paris.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Délinquance Bronze : {total_in:,} → {total_out:,} lignes Paris → {output_file.name}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    ingest_delinquance()
