# ingestion/api — Feeders API REST

Modules d'ingestion depuis des APIs REST externes. Ces scripts Python s'exécutent dans le conteneur `ude_api` et sont distincts des jobs Spark.

## Fichiers

| Fichier | Source | Destination | Volume |
|---------|--------|-------------|--------|
| `feeder_logements_sociaux.py` | opendata.paris.fr | `data/bronze/logements_sociaux_raw.json` | 4 174 records |
| `feeder_airparif_batch.py` | api.airparif.fr | MongoDB `air_quality` (TTL 24h) | 20 arrondissements |

## feeder_logements_sociaux.py

Appelle l'API opendata.paris.fr avec pagination automatique (100 records par page). Écrit le résultat brut dans `data/bronze/logements_sociaux_raw.json`. Ce JSON est ensuite lu par `pipelines/spark/feeder.py --source logements_sociaux` pour être chargé dans MinIO bronze.

- Retry avec backoff exponentiel via `tenacity` (3 tentatives max)
- Pagination gérée sur le paramètre `offset`

## feeder_airparif_batch.py

Effectue 20 appels async (`aiohttp` + `asyncio`) vers l'API Airparif, un par arrondissement (codes INSEE 75101–75120). Authentification via header `X-Api-Key`. Écrit le résultat directement dans MongoDB (`urban_data_nosql.air_quality`) en mode `replace_one upsert` sur le champ `insee`.

Pour le streaming continu via Kafka, voir `ingestion/streaming/`.

## Exécution

```powershell
# Logements sociaux → data/bronze/ (puis lancer Spark feeder.sh logements_sociaux)
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux

# Airparif batch → MongoDB directement
docker exec ude_api python ingestion/api/feeder_airparif_batch.py
```

## Dépendances

- `src/common/config.py` : URLs APIs, clé Airparif, paramètres MongoDB
- `src/common/exceptions.py` : `APIError`, `IngestionError`
- Techno : `requests`, `aiohttp`, `asyncio`, `tenacity`, `pymongo`
