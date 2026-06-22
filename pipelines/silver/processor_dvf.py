"""
Processor Silver — DVF (C2.3)

Lit les fichiers Bronze (Parquet) et produit data/silver/transactions_paris.parquet :
  - Calcul prix/m²
  - Nettoyage (suppression lignes sans surface ou prix)
  - Normalisation des types de local
  - Extraction du numéro d'arrondissement
  - Création de la géométrie Point (longitude, latitude)
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging
from src.common.utils import code_postal_to_arrondissement, log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Types de local à conserver pour l'analyse résidentielle
TYPES_LOCAL_RESIDENTIELS = {"Appartement", "Maison", "Dépendance"}

# Seuils de cohérence métier
PRIX_MIN = 10_000       # euros (en dessous : transaction suspecte)
PRIX_MAX = 50_000_000   # euros
SURFACE_MIN = 5         # m²
SURFACE_MAX = 2_000     # m²
PRIX_M2_MAX = 100_000   # euros/m² (au-delà : outlier)


@log_pipeline_run("silver_dvf", "dvf", "silver")
def process_dvf(bronze_path: str | None = None, silver_path: str | None = None) -> dict:
    """
    Transforme les fichiers Bronze DVF en Silver normalisé.
    """
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    silver_dir = Path(silver_path or settings.data_silver_path)
    silver_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(bronze_dir.glob("dvf_*.parquet"))
    if not parquet_files:
        logger.warning(f"Aucun fichier DVF Bronze trouvé dans {bronze_dir}")
        return {"nb_lignes_in": 0, "nb_lignes_out": 0}

    dfs = []
    total_in = 0

    for pf in sorted(parquet_files):
        df = pd.read_parquet(pf)
        total_in += len(df)
        dfs.append(df)
        logger.info(f"  Chargé {pf.name} : {len(df):,} lignes")

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total Bronze DVF : {total_in:,} lignes")

    # ── 1. Types et parsing date ──────────────────────────────────────────────
    df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
    df["valeur_fonciere"] = pd.to_numeric(df["valeur_fonciere"].astype(str).str.replace(",", "."), errors="coerce")
    df["surface_reelle_bati"] = pd.to_numeric(df["surface_reelle_bati"], errors="coerce")
    df["code_postal"] = df["code_postal"].astype(str).str.strip().str.zfill(5)

    # ── 2. Filtre résidentiel ─────────────────────────────────────────────────
    df = df[df["type_local"].isin(TYPES_LOCAL_RESIDENTIELS)]

    # ── 3. Suppression des lignes sans surface ou prix ───────────────────────
    df = df.dropna(subset=["valeur_fonciere", "surface_reelle_bati"])
    df = df[
        (df["valeur_fonciere"] >= PRIX_MIN) & (df["valeur_fonciere"] <= PRIX_MAX) &
        (df["surface_reelle_bati"] >= SURFACE_MIN) & (df["surface_reelle_bati"] <= SURFACE_MAX)
    ]

    # ── 4. Calcul prix/m² ────────────────────────────────────────────────────
    df["prix_m2"] = (df["valeur_fonciere"] / df["surface_reelle_bati"]).round(2)
    df = df[df["prix_m2"] <= PRIX_M2_MAX]

    # ── 5. Extraction arrondissement depuis code_postal ───────────────────────
    df["arrondissement"] = df["code_postal"].apply(code_postal_to_arrondissement)
    df = df[df["arrondissement"].notna()]
    df["arrondissement"] = df["arrondissement"].astype(int)

    # ── 6. Sélection et renommage des colonnes finales ────────────────────────
    df = df[[
        "date_mutation", "nature_mutation", "valeur_fonciere",
        "code_postal", "code_commune", "arrondissement",
        "type_local", "surface_reelle_bati", "nombre_pieces_principales",
        "prix_m2", "longitude", "latitude",
    ]].rename(columns={"surface_reelle_bati": "surface_m2", "nombre_pieces_principales": "nb_pieces"})

    total_out = len(df)
    output_file = silver_dir / "transactions_paris.parquet"
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Silver DVF : {total_in:,} → {total_out:,} lignes ({total_out/total_in*100:.1f}% conservées)")
    logger.info(f"Écrit : {output_file}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    process_dvf()
