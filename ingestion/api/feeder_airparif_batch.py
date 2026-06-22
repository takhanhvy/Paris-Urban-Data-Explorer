"""
Feeder Airparif batch (sans Kafka) — API live → MongoDB directement

Utile pour le premier chargement ou les tests.
Pour le streaming continu (toutes les heures), utiliser airparif_producer + consumer.
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from pymongo import MongoClient, ASCENDING
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)
settings = get_settings()


def _setup_collection(client: MongoClient):
    db = client[settings.mongo_db]
    collection = db["air_quality"]
    # Index TTL : expiration automatique après 24h
    collection.create_index(
        [("inserted_at", ASCENDING)],
        expireAfterSeconds=86400,
        name="ttl_24h",
    )
    collection.create_index([("insee", ASCENDING)], name="idx_insee")
    return collection


@retry(
    retry=retry_if_exception_type(aiohttp.ClientError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=2, max=15),
    reraise=True,
)
async def _fetch_one(session: aiohttp.ClientSession, insee: str) -> dict | None:
    url = f"{settings.airparif_base_url}?insee={insee}"
    headers = {"X-Api-Key": settings.airparif_api_key}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return {"insee": insee, "data": data}
    except Exception as e:
        logger.error(f"Erreur fetch Airparif INSEE={insee} : {e}")
        return None


async def fetch_all() -> list[dict]:
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_one(session, insee) for insee in settings.arrondissements_insee]
        results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]


def ingest_airparif_batch() -> dict:
    logger.info("Démarrage ingestion Airparif batch → MongoDB")

    results = asyncio.run(fetch_all())
    if not results:
        logger.error("Aucune donnée Airparif récupérée — vérifier la clé API dans .env")
        return {"nb_lignes_out": 0}

    client = MongoClient(settings.mongo_uri)
    collection = _setup_collection(client)

    now = datetime.now(timezone.utc)
    nb = 0
    for r in results:
        doc = {**r, "inserted_at": now, "timestamp": now.isoformat()}
        collection.replace_one({"insee": r["insee"]}, doc, upsert=True)
        nb += 1

    client.close()
    logger.info(f"Airparif batch terminé : {nb}/20 arrondissements chargés dans MongoDB")
    return {"nb_lignes_out": nb}


if __name__ == "__main__":
    ingest_airparif_batch()
