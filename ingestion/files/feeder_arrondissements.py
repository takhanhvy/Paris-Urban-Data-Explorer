"""
Feeder Arrondissements — CSV géographique vers data/bronze/

Lit arrondissements.csv (séparateur ;) depuis data/raw/,
joint avec la population 2020 INSEE (données de référence hardcodées),
sauvegarde dans data/bronze/arrondissements/.

Usage :
    python -m ingestion.files.feeder_arrondissements
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Population 2020 — source INSEE recensement (données de référence fixes)
POPULATION_2020 = {
    1: 16266,  2: 21559,  3: 34576,  4: 28088,  5: 58850,
    6: 41100,  7: 51765,  8: 36808,  9: 59895,  10: 90372,
    11: 147476, 12: 142327, 13: 181556, 14: 135964, 15: 233484,
    16: 165820, 17: 167476, 18: 195233, 19: 184389, 20: 196004,
}


@log_pipeline_run("feeder_arrondissements", "arrondissements", "bronze")
def ingest_arrondissements(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """Lit arrondissements.csv, joint la population, écrit en bronze."""
    try:
        import pandas as pd
    except ImportError:
        raise IngestionError("pandas requis : pip install pandas")

    raw_dir    = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path) / "arrondissements"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    src = raw_dir / "arrondissements.csv"
    if not src.exists():
        raise IngestionError(f"Fichier introuvable : {src}")

    logger.info(f"Lecture : {src}")
    df_raw = pd.read_csv(src, sep=";", encoding="utf-8")

    # Colonne numéro arrondissement : contient "Num", pas "INSEE"
    num_col = [c for c in df_raw.columns if "Num" in c and "INSEE" not in c]
    if not num_col:
        raise IngestionError(f"Colonne numéro arrondissement introuvable parmi : {list(df_raw.columns)}")
    num_col = num_col[0]

    df = pd.DataFrame({
        "arrondissement": df_raw[num_col].astype(int),
        "superficie_km2": (df_raw["Surface"].astype(float) / 1_000_000).round(3),
    })
    df = df[(df["arrondissement"] >= 1) & (df["arrondissement"] <= 20)].copy()

    # Jointure population
    df["population_totale"] = df["arrondissement"].map(POPULATION_2020)
    df["superficie_km2"] = df["superficie_km2"]
    df["densite_population"] = (df["population_totale"] / df["superficie_km2"]).round(0).astype(int)

    dst = bronze_dir / "arrondissements.csv"
    df.to_csv(dst, index=False, encoding="utf-8")

    logger.info(f"Arrondissements Bronze : {len(df)} arrondissements → {dst}")
    logger.info(f"\n{df[['arrondissement','superficie_km2','population_totale','densite_population']].to_string(index=False)}")

    return {"nb_arrondissements": len(df)}


if __name__ == "__main__":
    ingest_arrondissements()
