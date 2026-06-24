"""
Feeder Airparif — API temps reel vers data/bronze/

Appelle l'API Airparif pour les 20 arrondissements parisiens (INSEE 75101-75120),
recupere l'indice de qualite de l'air du jour et du lendemain,
sauvegarde en JSON date dans data/bronze/air_quality/.

C'est la seule source temps reel / live du projet (competence C2.2 streaming).
Retry automatique via tenacity.

URL : https://api.airparif.fr/indices/prevision/commune?insee=75101
Auth : Header X-Api-Key

Usage :
    python -m ingestion.api.feeder_airparif_batch
    python -m ingestion.api.feeder_airparif_batch --date 2026-06-24
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.exceptions import APIError, IngestionError
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# Codes INSEE des 20 arrondissements parisiens
ARRONDISSEMENTS_INSEE = [
    f"7510{i}" if i < 10 else f"751{i}" for i in range(1, 21)
]

# Delai entre appels API (Airparif limite a ~1 req/s)
DELAY_BETWEEN_CALLS = 1.0


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    reraise=True,
)
def _fetch_commune(insee: str, api_key: str) -> dict:
    """Appel API Airparif pour un code INSEE avec retry."""
    url = settings.airparif_base_url
    headers = {"X-Api-Key": api_key}
    params = {"insee": insee}

    response = requests.get(url, headers=headers, params=params, timeout=15)
    if response.status_code == 401:
        raise APIError("Airparif : cle API invalide (401)", status_code=401, url=url)
    if response.status_code != 200:
        raise APIError(
            f"Airparif HTTP {response.status_code} pour {insee}",
            status_code=response.status_code,
            url=url,
        )
    return response.json()


@log_pipeline_run("feeder_airparif_batch", "air_quality", "bronze")
def ingest_airparif(
    bronze_path: str | None = None,
    target_date: str | None = None,
) -> dict:
    """
    Fetche la qualite de l'air pour les 20 arrondissements.
    Sauvegarde : data/bronze/air_quality/airparif_YYYY-MM-DD.json
    """
    api_key = settings.airparif_api_key
    if not api_key:
        raise IngestionError(
            "Variable AIRPARIF_API_KEY manquante dans .env\n"
            "  Ajouter : AIRPARIF_API_KEY=0a0b677e-d8c9-1057-8172-0e8bd46793af"
        )

    bronze_dir = Path(bronze_path or settings.data_bronze_path) / "air_quality"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    run_date = target_date or date.today().isoformat()
    results = []
    errors = []

    logger.info(f"Airparif batch — {len(ARRONDISSEMENTS_INSEE)} arrondissements — date={run_date}")

    for insee in ARRONDISSEMENTS_INSEE:
        arr_num = int(insee[3:])  # '75101' -> 1, '75112' -> 12
        try:
            data = _fetch_commune(insee, api_key)
            # L'API retourne un dict {date: {global: ..., polluants: ...}}
            results.append({
                "insee": insee,
                "arrondissement": arr_num,
                "fetched_at": run_date,
                "data": data,
            })
            logger.info(f"  arr. {arr_num:2d} ({insee}) : OK")
        except Exception as e:
            logger.warning(f"  arr. {arr_num:2d} ({insee}) : ERREUR — {e}")
            errors.append({"insee": insee, "error": str(e)})

        time.sleep(DELAY_BETWEEN_CALLS)

    nb_ok = len(results)
    nb_err = len(errors)

    dst = bronze_dir / f"airparif_{run_date}.json"
    payload = {
        "date": run_date,
        "nb_ok": nb_ok,
        "nb_erreurs": nb_err,
        "arrondissements": results,
        "erreurs": errors,
    }
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"Airparif Bronze : {nb_ok}/20 arrondissements -> {dst}")
    if nb_err:
        logger.warning(f"  {nb_err} echecs : {[e['insee'] for e in errors]}")

    return {"nb_ok": nb_ok, "nb_erreurs": nb_err, "fichier": str(dst)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feeder Airparif batch")
    parser.add_argument("--date", help="Date cible YYYY-MM-DD (defaut: aujourd'hui)")
    args = parser.parse_args()
    ingest_airparif(target_date=args.date)
