# ingestion/streaming — Composant streaming Airparif (C2.2)

Module de streaming temps réel : API Airparif → Kafka → MongoDB.

## Architecture

```
airparif_producer.py              airparif_consumer.py
  (asyncio + aiohttp)    →    →     (confluent-kafka)    →    MongoDB
  20 appels / heure        Kafka      topic: airparif.quality    TTL 24h
                           topic
```

## Fichiers

| Fichier | Rôle |
|---------|------|
| `airparif_producer.py` | Appelle les 20 endpoints Airparif en async, publie dans Kafka |
| `airparif_consumer.py` | Consomme le topic Kafka, écrit dans MongoDB avec index TTL |

## Pourquoi Kafka ?

Le log Kafka permet de **rejouer** les messages et de **découpler** la collecte de la persistance. Si MongoDB est temporairement indisponible, les messages restent dans Kafka (rétention 24h) et sont consommés au redémarrage — sans perte de données.

## Techno

`aiohttp`, `asyncio`, `confluent-kafka`, `pymongo`, `tenacity`

## Dépendances

- `src/common/config.py` — clé API Airparif, bootstrap servers Kafka, URI MongoDB
- Service Kafka opérationnel (`docker compose up -d kafka`)
- Service MongoDB opérationnel (`docker compose up -d mongodb`)

## Exécution

```powershell
# Terminal 1 — Producer (appelle Airparif → Kafka)
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer (Kafka → MongoDB)
python -m ingestion.streaming.airparif_consumer
```

## Alternative batch

`ingestion/api/feeder_airparif_batch.py` injecte les données Airparif directement dans MongoDB sans Kafka — utile pour le premier chargement ou les tests.
