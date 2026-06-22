# ingestion/streaming — Composant streaming Airparif (C2.2)

Module de streaming temps réel : API Airparif → Kafka → MongoDB.

## Architecture

```
airparif_producer.py          airparif_consumer.py
  (asyncio + aiohttp)    →      (confluent-kafka)    →    MongoDB
  20 appels / heure        topic: airparif.quality        TTL 24h
```

## Fichiers

| Fichier | Rôle |
|---------|------|
| `airparif_producer.py` | Appelle les 20 endpoints Airparif en async, publie dans Kafka |
| `airparif_consumer.py` | Consomme le topic Kafka, stocke dans MongoDB avec index TTL |

## Pourquoi Kafka ?

Le log Kafka permet de **rejouer** les données et de **découpler** la collecte de la persistance. Si MongoDB est temporairement indisponible, les messages restent dans Kafka et sont consommés au redémarrage.

## Techno

`aiohttp`, `asyncio`, `confluent-kafka`, `pymongo`, `tenacity`

## Dépendances

- `src/common/config.py` — API key Airparif, bootstrap servers Kafka
- Service Kafka opérationnel (`docker-compose up kafka`)
- Service MongoDB opérationnel (`docker-compose up mongodb`)

## Exécution

```bash
# Terminal 1 — Producer
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer
python -m ingestion.streaming.airparif_consumer

# ou
make ingest-stream
```
