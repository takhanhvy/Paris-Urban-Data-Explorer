# ingestion/streaming — Streaming Airparif via Kafka (C2.2)

Composant temps réel : collecte des indices de qualité de l'air Airparif via Kafka.

## Architecture

```
airparif_producer.py          topic Kafka           airparif_consumer.py
  asyncio + aiohttp       →   airparif.quality   →    confluent-kafka
  20 appels / cycle                                    → MongoDB air_quality
  codes INSEE 75101–75120                                TTL 24h
```

## Fichiers

| Fichier | Rôle |
|---------|------|
| `airparif_producer.py` | Appelle les 20 endpoints Airparif en async et publie chaque réponse JSON dans le topic Kafka `airparif.quality` |
| `airparif_consumer.py` | Consomme le topic Kafka et écrit dans MongoDB via `replace_one upsert` sur le champ `insee` |

## Pourquoi Kafka

Le log Kafka découple la collecte de la persistance : si MongoDB est temporairement indisponible, les messages restent dans le topic (rétention 24h configurée) et sont consommés au redémarrage sans perte. Le topic peut aussi alimenter d'autres consumers (alertes, archivage S3) sans modifier le producer.

## Exécution

Les deux processus tournent en parallèle (deux terminaux ou deux conteneurs) :

```powershell
# Terminal 1 — Producer
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer
python -m ingestion.streaming.airparif_consumer
```

## Alternative batch

`ingestion/api/feeder_airparif_batch.py` injecte les données Airparif directement dans MongoDB sans passer par Kafka. Utile pour le premier chargement ou les tests.

## Dépendances

- `src/common/config.py` : clé API Airparif, bootstrap servers Kafka, URI MongoDB
- Services requis : `ude_zookeeper`, `ude_kafka`, `ude_mongodb` (`docker compose up -d`)
- Techno : `aiohttp`, `asyncio`, `confluent-kafka`, `pymongo`, `tenacity`
