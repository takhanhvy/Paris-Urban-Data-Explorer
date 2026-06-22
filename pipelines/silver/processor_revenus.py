"""
Processor Silver — Revenus FiLoSoFi (C2.3)

Lit data/bronze/revenus_iris_raw.parquet et produit
data/silver/revenus_iris_paris.parquet :
  - Filtre sur Paris (code commune 751xx)
  - Sélection des 12 colonnes utiles sur 28
  - Extraction arrondissement depuis code commune
  - Normalisation des types numériques
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

# Colonnes à conserver (12 sur 28)
COLS_UTILES = [
    "IRIS", "LIBIRIS", "COM", "LIBCOM",
    "DEC_MED18",   # revenu médian
    "DEC_Q118",    # 1er quartile
    "DEC_Q318",    # 3e quartile
    "DEC_D118",    # 1er décile
    "DEC_D918",    # 9e décile
    "DEC_GI18",    # indice de Gini
    "DEC_RD18",    # rapport interdécile
    "DEC_PACT18",  # part revenus d'activité
    "DEC_PCHO18",  # part indemnités chômage
    "DEC_PPEN18",  # part pensions/retraites
    "DEC_S80S2018",# rapport S80/S20
]

# Mapping vers noms SQL
RENAME_MAP = {
    "IRIS": "code_iris",
    "LIBIRIS": "nom_iris",
    "COM": "code_commune",
    "LIBCOM": "nom_commune",
    "DEC_MED18": "revenu_median",
    "DEC_Q118": "q1",
    "DEC_Q318": "q3",
    "DEC_D118": "d1",
    "DEC_D918": "d9",
    "DEC_GI18": "indice_gini",
    "DEC_RD18": "rapport_interdecile",
    "DEC_PACT18": "part_revenus_activ",
    "DEC_PCHO18": "part_chomage",
    "DEC_PPEN18": "part_retraites",
    "DEC_S80S2018": "rapport_s80s20",
}


@log_pipeline_run("silver_revenus", "revenus", "silver")
def process_revenus(bronze_path: str | None = None, silver_path: str | None = None) -> dict:
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    silver_dir = Path(silver_path or settings.data_silver_path)
    silver_dir.mkdir(parents=True, exist_ok=True)

    source_file = bronze_dir / "revenus_iris_raw.parquet"
    if not source_file.exists():
        raise IngestionError(f"Fichier Bronze introuvable : {source_file}")

    df = pd.read_parquet(source_file)
    total_in = len(df)
    logger.info(f"Revenus Bronze : {total_in:,} lignes")

    # ── 1. Filtre Paris (déjà fait en Bronze, mais on s'assure) ───────────────
    df["COM"] = df["COM"].astype(str).str.strip().str.zfill(5)
    df = df[df["COM"].str.startswith("751")].copy()

    # ── 2. Sélection colonnes utiles ──────────────────────────────────────────
    cols_present = [c for c in COLS_UTILES if c in df.columns]
    df = df[cols_present].copy()

    # ── 3. Renommage ──────────────────────────────────────────────────────────
    df = df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})

    # ── 4. Extraction arrondissement depuis code_commune ──────────────────────
    # code_commune = '75101' → arrondissement = 1
    df["arrondissement"] = df["code_commune"].apply(
        lambda c: int(str(c)[3:]) if str(c).startswith("751") and str(c)[3:].isdigit() else None
    )
    df = df[df["arrondissement"].between(1, 20)]

    # ── 5. Normalisation numériques ───────────────────────────────────────────
    numeric_cols = [
        "revenu_median", "q1", "q3", "d1", "d9",
        "indice_gini", "rapport_interdecile",
        "part_revenus_activ", "part_chomage", "part_retraites", "rapport_s80s20",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["annee"] = 2018

    total_out = len(df)
    output_file = silver_dir / "revenus_iris_paris.parquet"
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Silver revenus : {total_in:,} → {total_out:,} lignes → {output_file.name}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    process_revenus()
