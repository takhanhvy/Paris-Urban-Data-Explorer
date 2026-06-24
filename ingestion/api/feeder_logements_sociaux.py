"""
Feeder Logements Sociaux — API opendata.paris.fr vers data/raw/
4 174 enregistrements, paginés par 100.

Sortie : data/raw/logements_sociaux_raw.json (landing zone, fidèle à la source)
Retry automatique via tenacity (3 tentatives, backoff exponentiel).
"""

import json
import sys
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

PAGE_SIZE = 100


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _fetch_page(url: str, params: dict) -> dict:
    """Appel API avec retry automatique."""
    response = requests.get(url, params=params, timeout=30)
    if response.status_code != 200:
        raise APIError(
            f"API logements sociaux : HTTP {response.status_code}",
            status_code=response.status_code,
            url=url,
        )
    return response.json()


@log_pipeline_run("feeder_logements_sociaux", "logements_sociaux", "raw")
def ingest_logements_sociaux(raw_path: str | None = None) -> dict:
    """
    Pagine l'API Paris OpenData et sauvegarde les résultats bruts en JSON
    dans la landing zone data/raw/ (lu ensuite par Spark feeder.py).
    """
    raw_dir = Path(raw_path or settings.data_raw_path)
    raw_dir.mkdir(parents=True, exist_ok=True)

    url = settings.paris_opendata_url
    all_records = []
    offset = 0

    logger.info(f"Démarrage ingestion logements sociaux — {url}")

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order_by": "id_livraison",
        }
        try:
            data = _fetch_page(url, params)
        except Exception as e:
            raise IngestionError(f"Échec fetch page offset={offset}: {e}") from e

        records = data.get("results", [])
        if not records:
            break

        all_records.extend(records)
        total = data.get("total_count", "?")
        logger.info(f"  Page offset={offset} — {len(records)} records ({len(all_records)}/{total})")

        offset += PAGE_SIZE
        if len(all_records) >= int(total):
            break

    # Sauvegarde JSON brut dans la landing zone raw/ (fidèle à la source)
    output_file = raw_dir / "logements_sociaux_raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    logger.info(f"Logements sociaux raw : {len(all_records)} enregistrements → {output_file}")
    return {"nb_lignes_total": len(all_records)}


if __name__ == "__main__":
    ingest_logements_sociaux()
