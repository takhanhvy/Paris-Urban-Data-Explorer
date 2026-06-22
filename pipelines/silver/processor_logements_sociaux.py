"""
Processor Silver — Logements sociaux (C2.3)

Lit data/bronze/logements_sociaux_raw.json et produit
data/silver/logements_sociaux.parquet :
  - Reprojection Lambert93 → WGS84 (pyproj)
  - Standardisation arrondissement (int 1–20)
  - Suppression doublons sur id_livraison
"""

import json
import sys
from pathlib import Path

import pandas as pd
from pyproj import Transformer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Transformer Lambert93 (EPSG:2154) → WGS84 (EPSG:4326)
TRANSFORMER = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


@log_pipeline_run("silver_logements_sociaux", "logements_sociaux", "silver")
def process_logements_sociaux(bronze_path: str | None = None, silver_path: str | None = None) -> dict:
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    silver_dir = Path(silver_path or settings.data_silver_path)
    silver_dir.mkdir(parents=True, exist_ok=True)

    source_file = bronze_dir / "logements_sociaux_raw.json"
    if not source_file.exists():
        raise IngestionError(f"Fichier Bronze introuvable : {source_file}")

    with open(source_file, encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    total_in = len(df)
    logger.info(f"Logements sociaux Bronze : {total_in} enregistrements")

    # ── 1. Extraction arrondissement ──────────────────────────────────────────
    # La colonne 'arrdt' contient des valeurs comme '1', '2', ..., '20'
    df["arrondissement"] = pd.to_numeric(df.get("arrdt"), errors="coerce").astype("Int64")

    # ── 2. Reprojection Lambert93 → WGS84 ────────────────────────────────────
    # Colonnes source : coord_x_l93, coord_y_l93
    mask_l93 = df["coord_x_l93"].notna() & df["coord_y_l93"].notna()
    if mask_l93.any():
        x = pd.to_numeric(df.loc[mask_l93, "coord_x_l93"], errors="coerce")
        y = pd.to_numeric(df.loc[mask_l93, "coord_y_l93"], errors="coerce")
        lon, lat = TRANSFORMER.transform(x.values, y.values)
        df.loc[mask_l93, "longitude"] = lon
        df.loc[mask_l93, "latitude"] = lat
    else:
        # Fallback : utiliser geo_point_2d si disponible
        if "geo_point_2d" in df.columns:
            df["longitude"] = df["geo_point_2d"].apply(
                lambda p: p.get("lon") if isinstance(p, dict) else None
            )
            df["latitude"] = df["geo_point_2d"].apply(
                lambda p: p.get("lat") if isinstance(p, dict) else None
            )

    # ── 3. Nettoyage colonnes numériques ──────────────────────────────────────
    for col in ["nb_logmt_total", "nb_plai", "nb_plus", "nb_pluscd", "nb_pls"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["annee"] = pd.to_numeric(df.get("annee"), errors="coerce").astype("Int64")

    # ── 4. Suppression doublons ───────────────────────────────────────────────
    df = df.drop_duplicates(subset=["id_livraison"], keep="first")

    # ── 5. Filtre arrondissements valides ─────────────────────────────────────
    df = df[df["arrondissement"].between(1, 20)]

    # ── 6. Sélection colonnes finales ─────────────────────────────────────────
    cols = [
        "id_livraison", "adresse_programme", "code_postal", "arrondissement",
        "annee", "nb_logmt_total", "nb_plai", "nb_plus", "nb_pluscd", "nb_pls",
        "mode_real", "nature_programme", "commentaires", "longitude", "latitude",
    ]
    cols_present = [c for c in cols if c in df.columns]
    df = df[cols_present]

    total_out = len(df)
    output_file = silver_dir / "logements_sociaux.parquet"
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Silver logements sociaux : {total_in} → {total_out} lignes → {output_file.name}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    process_logements_sociaux()
