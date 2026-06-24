# ingestion/api — Feeders API REST

Modules d'ingestion depuis des APIs REST externes.

## Contenu

| Fichier | Source | Destination | Volume |
|---------|--------|-------------|--------|
| `feeder_logements_sociaux.py` | opendata.paris.fr | `data/bronze/logements_sociaux_raw.json` | 4 174 records |
| `feeder_airparif_batch.py` | api.airparif.fr | MongoDB `air_quality` | 20 arrondissements |

## Flux logements sociaux

```
API opendata.paris.fr
    │  (pagination 100 records/page, retry tenacity)
    ▼
data/bronze/logements_sociaux_raw.json
    │
    ▼  spark-submit feeder.py --source logements_sociaux
MinIO  s3a://urban-data/raw/logements_sociaux/
```

## Flux Airparif (batch direct)

```
API api.airparif.fr  (20 appels async, X-Api-Key)
    │
    ▼
MongoDB  urban_data_nosql.air_quality  (TTL 24h, upsert par insee)
```

Pour le streaming continu Airparif via Kafka, voir `ingestion/streaming/`.

## Comportement

- Pagination automatique (100 records / page) pour les logements sociaux
- Retry avec backoff exponentiel via `tenacity` (3 tentatives max)
- Appels Airparif en `asyncio` (20 requêtes simultanées)

## Techno

`requests`, `aiohttp`, `asyncio`, `tenacity`, `pymongo`

## Dépendances

- `src/common/config.py` — URLs, clé API Airparif, paramètres MongoDB
- `src/common/utils.py` — décorateur `@log_pipeline_run`
- `src/common/exceptions.py` — `APIError`, `IngestionError`

## Exécution

```powershell
# Logements sociaux → data/bronze/ (puis Spark feeder.sh logements_sociaux)
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux

# Airparif batch → MongoDB directement
docker exec ude_api python ingestion/api/feeder_airparif_batch.py
```
