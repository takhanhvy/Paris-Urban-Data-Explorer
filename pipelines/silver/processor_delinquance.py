"""
Processor Silver — Délinquance (C2.3)

Lit data/bronze/delinquance_raw.parquet et produit
data/silver/delinquance_paris.parquet :
  - Extraction numéro arrondissement depuis CODGEO_2025
  - Normalisation des types
  - Filtre sur les codes Paris (75101–75120)
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import insee_to_arrondissement, log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()


@log_pipeline_run("silver_delinquance", "delinquance", "silver")
def process_delinquance(bronze_path: str | None = None, silver_path: str | None = None) -> dict:
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    silver_dir = Path(silver_path or settings.data_silver_path)
    silver_dir.mkdir(parents=True, exist_ok=True)

    source_file = bronze_dir / "delinquance_raw.parquet"
    if not source_file.exists():
        raise IngestionError(f"Fichier Bronze introuvable : {source_file}")

    df = pd.read_parquet(source_file)
    total_in = len(df)
    logger.info(f"Délinquance Bronze : {total_in:,} lignes")

    # ── 1. Normalisation code commune ─────────────────────────────────────────
    codgeo_col = "CODGEO_2025"
    df[codgeo_col] = df[codgeo_col].astype(str).str.strip()

    # ── 2. Extraction arrondissement ──────────────────────────────────────────
    df["arrondissement"] = df[codgeo_col].apply(insee_to_arrondissement)
    df = df[df["arrondissement"].notna()]
    df["arrondissement"] = df["arrondissement"].astype(int)

    # ── 3. Normalisation colonnes numériques ──────────────────────────────────
    df["annee"] = pd.to_numeric(df.get("annee"), errors="coerce").astype("Int64")
    df["nombre"] = pd.to_numeric(df.get("nombre"), errors="coerce")
    df["taux_pour_mille"] = pd.to_numeric(df.get("taux_pour_mille"), errors="coerce")

    # ── 4. Renommage pour cohérence avec le schéma SQL ────────────────────────
    df = df.rename(columns={codgeo_col: "code_insee"})

    # ── 5. Sélection colonnes finales ─────────────────────────────────────────
    cols = [
        "annee", "arrondissement", "code_insee", "indicateur",
        "nombre", "taux_pour_mille", "unite_de_compte",
        "est_diffuse", "insee_log", "insee_pop",
    ]
    cols_present = [c for c in cols if c in df.columns]
    df = df[cols_present].drop_duplicates()

    total_out = len(df)
    output_file = silver_dir / "delinquance_paris.parquet"
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Silver délinquance : {total_in:,} → {total_out:,} lignes → {output_file.name}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    process_delinquance()
