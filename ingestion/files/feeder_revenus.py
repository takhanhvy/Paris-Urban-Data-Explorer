"""
Feeder Revenus — XLSX INSEE FiLoSoFi 2018 vers data/bronze/

Lit BASE_TD_FILO_DEC_IRIS_2018.xlsx, filtre sur Paris (COM LIKE '751%'),
et sauvegarde dans data/bronze/revenus_iris_raw.parquet.
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
COM_COL = "COM"  # colonne code commune


@log_pipeline_run("feeder_revenus", "revenus", "bronze")
def ingest_revenus(raw_path: str | None = None, bronze_path: str | None = None) -> dict:
    """
    Lit le fichier XLSX FiLoSoFi, filtre Paris, convertit en Parquet Bronze.
    """
    raw_dir = Path(raw_path or settings.data_raw_path)
    bronze_dir = Path(bronze_path or settings.data_bronze_path)
    bronze_dir.mkdir(parents=True, exist_ok=True)

    xlsx_file = raw_dir / XLSX_FILENAME
    if not xlsx_file.exists():
        # Cherche n'importe quel XLSX si le nom exact n'est pas trouvé
        xlsx_files = list(raw_dir.glob("*.xlsx"))
        if not xlsx_files:
            raise IngestionError(f"Aucun fichier XLSX trouvé dans {raw_dir}")
        xlsx_file = xlsx_files[0]
        logger.warning(f"Fichier {XLSX_FILENAME} non trouvé, utilisation de {xlsx_file.name}")

    logger.info(f"Lecture de {xlsx_file.name} (peut prendre quelques secondes…)")

    df = pd.read_excel(
        xlsx_file,
        dtype={COM_COL: str, "IRIS": str},
        engine="openpyxl",
        sheet_name="IRIS_DEC", 
        header=5
    )
    total_in = len(df)
    logger.info(f"  Total national : {total_in:,} lignes")

    # Filtre Paris : codes commune commençant par '751'
    df[COM_COL] = df[COM_COL].astype(str).str.strip().str.zfill(5)
    df_paris = df[df[COM_COL].str.startswith("751")].copy()
    total_out = len(df_paris)

    output_file = bronze_dir / "revenus_iris_raw.parquet"
    df_paris.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Revenus Bronze : {total_in:,} → {total_out:,} lignes Paris → {output_file.name}")
    return {"nb_lignes_in": total_in, "nb_lignes_out": total_out}


if __name__ == "__main__":
    ingest_revenus()
