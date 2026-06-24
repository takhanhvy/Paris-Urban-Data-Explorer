"""
Feeder Delinquance — Parquet (France entiere) vers data/bronze/

Lit le fichier delinquance.parquet depuis data/raw/ (France entiere),
filtre sur les codes INSEE parisiens 75101-75120,
sauvegarde en Parquet dans data/bronze/delinquance/.

Colonnes source :
    annee, nombre, indicateur, CODGEO_2025, taux_pour_mille,
    unite_de_compte, est_diffuse, insee_log, insee_pop,
    complement_info_taux, complement_info_nombre

Usage :
    python -m ingestion.files.feeder_delinquance
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

# Codes INSEE des 20 arrondissements parisiens
PARIS_INSEE = {f"7510{i}" if i < 10 else f"751{i}" for i in range(1, 21)}


@log_pipeline_run("feeder_delinquance", "delinquance", "bronze")
def ingest_delinquance(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """Lit le parquet France entiere, filtre Paris, ecrit bronze Parquet."""
    raw_dir = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path) / "delinquance"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # Cherche le fichier parquet (nom variable selon televersement)
    parquet_files = list(raw_dir.glob("*.parquet"))
    if not parquet_files:
        raise IngestionError(f"Aucun fichier .parquet trouve dans {raw_dir}")

    # Si plusieurs parquets, prendre celui qui contient "delinquance" ou le premier
    src = next(
        (f for f in parquet_files if "delinquance" in f.name.lower()),
        parquet_files[0],
    )
    logger.info(f"Lecture : {src}")

    df_raw = pd.read_parquet(src, engine="pyarrow")
    nb_raw = len(df_raw)
    logger.info(f"  France entiere : {nb_raw:,} enregistrements")

    # Filtrage Paris — colonne CODGEO_2025 contient les codes INSEE
    geo_col = next(
        (c for c in df_raw.columns if "CODGEO" in c.upper() or "codgeo" in c.lower()),
        None,
    )
    if geo_col is None:
        raise IngestionError(f"Colonne CODGEO introuvable parmi : {list(df_raw.columns)}")

    df_paris = df_raw[df_raw[geo_col].astype(str).isin(PARIS_INSEE)].copy()
    nb_paris = len(df_paris)
    logger.info(f"  Paris (75101-75120) : {nb_paris:,} enregistrements")

    if nb_paris == 0:
        raise IngestionError("Aucun enregistrement Paris apres filtrage — verifier le fichier source")

    dst = bronze_dir / "delinquance_paris.parquet"
    df_paris.to_parquet(dst, index=False, engine="pyarrow")
    logger.info(f"Delinquance Bronze : {nb_paris:,} lignes Paris -> {dst}")

    return {"nb_france": nb_raw, "nb_paris": nb_paris}


if __name__ == "__main__":
    ingest_delinquance()
