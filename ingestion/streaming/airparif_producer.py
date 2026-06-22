"""
Airparif Producer — API live qualité de l'air → Kafka topic (C2.2)

Appelle les 20 endpoints Airparif (un par arrondissement) de manière asynchrone
toutes les heures et publie les résultats dans le topic Kafka 'airparif.quality'.

Lancer avec : python -m ingestion.streaming.airparif_producer
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from confluent_kafka import Producer
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

ARRONDISSEMENTS = settings.arrondissements_insee
POLL_INTERVAL = 3600  # secondes (1 heure)


def _make_producer() -> Producer:
    """Initialise le producteur Kafka."""
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "retries": 3,
        "acks": "all",
    })


@retry(
    retry=retry_if_exception_type(aiohttp.ClientError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    reraise=True,
)
async def _fetch_air_quality(session: aiohttp.ClientSession, insee: str) -> dict:
    """Appel async à l'API Airparif pour un arrondissement."""
    url = f"{settings.airparif_base_url}?insee={insee}"
    headers = {"X-Api-Key": settings.airparif_api_key}
    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return {"insee": insee, "timestamp": datetime.now(timezone.utc).isoformat(), "data": data}


async def poll_once(producer: Producer) -> None:
    """Récupère la qualité de l'air pour les 20 arrondissements et publie dans Kafka."""
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_air_quality(session, insee) for insee in ARRONDISSEMENTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    success = 0
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Erreur fetch Airparif : {result}")
            continue

        message = json.dumps(result, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic=settings.kafka_topic_airparif,
            key=result["insee"].encode("utf-8"),
            value=message,
        )
        success += 1

    producer.flush()
    logger.info(f"Airparif : {success}/{len(ARRONDISSEMENTS)} messages publiés dans Kafka")


async def run_producer() -> None:
    """Boucle principale : poll toutes les POLL_INTERVAL secondes."""
    producer = _make_producer()
    logger.info(f"Airparif producer démarré — intervalle : {POLL_INTERVAL}s")

    while True:
        try:
            await poll_once(producer)
        except Exception as e:
            logger.error(f"Erreur dans la boucle producer : {e}")

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(run_producer())
