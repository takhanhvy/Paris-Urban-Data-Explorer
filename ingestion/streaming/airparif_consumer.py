"""
Airparif Consumer — topic Kafka → MongoDB (C2.2)

Consomme les messages du topic 'airparif.quality' et les stocke
dans MongoDB avec un index TTL de 24h (les données expirent automatiquement).

Lancer avec : python -m ingestion.streaming.airparif_consumer
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)
settings = get_settings()


def _setup_mongo_collection(client: MongoClient):
    """
    Configure la collection MongoDB avec index TTL (expiration 24h)
    et index sur le code INSEE pour les requêtes rapides.
    """
    db = client[settings.mongo_db]
    collection = db["air_quality"]

    # Index TTL : documents supprimés automatiquement après 86400 secondes
    collection.create_index(
        [("inserted_at", ASCENDING)],
        expireAfterSeconds=86400,
        name="ttl_24h",
    )
    # Index sur le code INSEE pour GET /qualite-air/live?arrondissement=X
    collection.create_index([("insee", ASCENDING)], name="idx_insee")
    return collection


def run_consumer() -> None:
    """Boucle principale du consumer Kafka → MongoDB."""
    mongo_client = MongoClient(settings.mongo_uri)
    collection = _setup_mongo_collection(mongo_client)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": "ude_airparif_consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([settings.kafka_topic_airparif])

    logger.info(f"Consumer démarré — topic : {settings.kafka_topic_airparif}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error : {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                document = {
                    **payload,
                    "inserted_at": datetime.now(timezone.utc),  # champ pour le TTL index
                }
                # Upsert : on remplace le document existant pour ce code INSEE
                collection.replace_one(
                    {"insee": payload["insee"]},
                    document,
                    upsert=True,
                )
                logger.debug(f"Stocké qualité air INSEE={payload['insee']}")
            except (json.JSONDecodeError, PyMongoError) as e:
                logger.error(f"Erreur traitement message : {e}")

    except KeyboardInterrupt:
        logger.info("Consumer arrêté")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    run_consumer()
