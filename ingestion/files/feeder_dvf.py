"""
Feeder DVF — ingestion des fichiers CSV vers data/bronze/
Source : 5 fichiers CSV (2020–2025) déposés dans data/raw/

Rôle Bronze : convertir CSV → Parquet sans transformation métier.
"""

import sys
from pathlib import Path

import pandas as pd

# Ajouter la racine du projet au path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Colonnes à conserver depuis le CSV DVF (réduction volume)
DVF_COLUMNS = [
    "date_mutation",
    "nature_mutation",
    "valeur_fonciere",
    "code_postal",
    "code_commune",
    "type_local",
    "surface_reelle_bati",
    "nombre_pieces_principales",
    "longitude",
    "latitude",
]

# Filtre : uniquement Paris (codes commune 75xxx)
PARIS_COMMUNE_PREFIX = "75"


@log_pipeline_run("feeder_dvf", "dvf", "bronze")
def ingest_dvf(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """
    Lit les fichiers CSV DVF depuis data/raw/ et les sauvegarde en Parquet dans data/bronze/.

    Retourne un dict avec les métriques (nb_lignes, nb_fichiers).
    """
    raw_dir = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"Aucun fichier CSV trouvé dans {raw_dir}")
        return {"nb_fichiers": 0, "nb_lignes_total": 0}

    total_lignes = 0
    fichiers_traites = 0

    for csv_file in sorted(csv_files):
        logger.info(f"Lecture de {csv_file.name}")
        try:
            df = pd.read_csv(
                csv_file,
                sep=",",
                low_memory=False,
                usecols=lambda c: c in DVF_COLUMNS,
                dtype={"code_commune": str, "code_postal": str},
            )

            # Filtre Paris dès le Bronze (réduction volume)
            df = df[df["code_commune"].str.startswith(PARIS_COMMUNE_PREFIX, na=False)]

            nb_lignes = len(df)
            total_lignes += nb_lignes

            # Nommage : dvf_2024.parquet à partir du nom de fichier
            annee = _extract_annee(csv_file.stem)
            output_file = bronze_dir / f"dvf_{annee}.parquet"

            df.to_parquet(output_file, index=False, compression="snappy")
            fichiers_traites += 1
            logger.info(f"  → {nb_lignes:,} lignes Paris → {output_file.name}")

        except Exception as e:
            raise IngestionError(f"Erreur lecture {csv_file.name}: {e}") from e

    logger.info(f"DVF Bronze terminé : {fichiers_traites} fichiers, {total_lignes:,} lignes Paris")
    return {"nb_fichiers": fichiers_traites, "nb_lignes_total": total_lignes}


def _extract_annee(stem: str) -> str:
    """Extrait l'année depuis le nom de fichier (ex. 'vf_2024' → '2024')."""
    import re
    match = re.search(r"(20\d{2})", stem)
    return match.group(1) if match else "unknown"


if __name__ == "__main__":
    ingest_dvf()
