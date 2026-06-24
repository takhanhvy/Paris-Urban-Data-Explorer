# Urban Data Explorer

Plateforme data complète pour analyser le marché immobilier parisien : prix/m², logements sociaux, délinquance, qualité de l'air et revenus par arrondissement — alimentée par un pipeline Spark distribué.

## Stack technique

| Couche | Techno | Rôle |
|--------|--------|------|
| Compute distribué | Apache Spark 3.5 Standalone | Ingestion, nettoyage, agrégation (jobs Python) |
| Data Lake | HDFS (Hadoop Distributed File System) | Couches raw et silver, partitionnées par date |
| Base relationnelle | PostgreSQL 15 + PostGIS | Couche gold + référentiel géospatial |
| Base NoSQL | MongoDB 7 | Données Airparif live (TTL 24h) |
| Streaming | Apache Kafka | Pipeline Airparif temps réel (C2.2) |
| API | FastAPI + pg8000 | REST, Swagger auto-généré |
| Dashboard | JavaScript + MapLibre GL JS | Carte choroplèthe, graphiques, timeline |
| Conteneurisation | Docker Compose | Stack complète en un seul fichier |

## Sources de données (5 types)

| # | Source | Format | Volume |
|---|--------|--------|--------|
| 1 | DVF — valeurs foncières 2020–2025 | CSV (pipe-séparé) | ~millions de lignes |
| 2 | Logements sociaux Paris | API REST opendata.paris.fr | 4 174 records |
| 3 | Délinquance INSEE | Parquet (France → filtre Paris) | — |
| 4 | Qualité de l'air Airparif | API live (20 arrondissements) | Temps réel |
| 5 | Revenus FiLoSoFi INSEE 2018 | XLSX (12 395 IRIS) | — |
| + | Arrondissements | CSV (superficie) + XLSX (population) | 20 lignes |

## Architecture Medallion

```
data/raw/  +  data/bronze/         (fichiers locaux sources)
    │
    ▼  spark-submit feeder.py
HDFS  hdfs://namenode:9000/urban-data/raw/{source}/
          ingestion_year=.../ingestion_month=.../ingestion_day=...
    │
    ▼  spark-submit processor.py   (nettoyage, cast types, filtre Paris)
HDFS  hdfs://namenode:9000/urban-data/silver/{source}/
    │
    ▼  spark-submit datamart.py    (agrégations, score attractivité, JDBC)
PostgreSQL  ude.indicateurs_gold
    │
    ▼
FastAPI  →  Dashboard MapLibre

API Airparif (live)  →  Kafka  →  MongoDB  →  FastAPI /api/air-quality
```

## Démarrage rapide

```powershell
# 1. Variables d'environnement
Copy-Item .env.example .env   # puis éditer AIRPARIF_API_KEY si nécessaire

# 2. Build et lancer la stack (première fois : build images Spark)
docker compose up -d --build

# 3. Vérifier que tout est healthy
docker compose ps

# 4. Ingestion Airparif (données live → MongoDB)
docker exec ude_api python ingestion/api/feeder_airparif_batch.py

# 5. Ingestion logements sociaux (API → bronze)
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux

# 6. Pipeline Spark complet (raw → silver → gold)
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh all
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh all
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh
```

Toutes les commandes détaillées dans [`docs/commands.md`](docs/commands.md).

## Interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| Dashboard | http://localhost:8000/dashboard | Carte choroplèthe + graphiques |
| API docs | http://localhost:8000/docs | Swagger OpenAPI auto-généré |
| Spark UI | http://localhost:8080 | Jobs, stages, cache Storage |
| HDFS NameNode | http://localhost:9870 | Explorateur fichiers distribués |

## Structure du projet

```
Paris-Urban-Data-Explorer/
├── data/raw/                   Fichiers sources (CSV DVF, parquet délinquance, XLSX revenus)
├── data/bronze/                JSON logements sociaux (sortie feeder API)
├── pipelines/spark/            Jobs Spark Python + scripts spark-submit
│   ├── feeder.py               Raw sources → MinIO /raw
│   ├── processor.py            MinIO /raw → MinIO /silver (nettoyage)
│   ├── datamart.py             MinIO /silver → PostgreSQL (agrégats + score)
│   └── submit/                 feeder.sh, processor.sh, datamart.sh
├── ingestion/
│   ├── api/                    Feeders API REST (logements sociaux, Airparif batch)
│   └── streaming/              Airparif → Kafka → MongoDB (temps réel)
├── src/api/                    FastAPI — endpoints + dashboard HTML
├── src/common/                 Config, logger, exceptions partagés
├── sql/ddl/                    Schéma PostgreSQL (00 → 07)
├── dashboard/                  Frontend MapLibre GL JS
├── docs/                       Architecture, data model, commandes
├── models/                     Documentation star schema
├── monitoring/                 Logs applicatifs (structlog JSON)
├── orchestration/              Documentation orchestration (spark-submit → Phase 2)
└── config/                     dev.yaml, logging.yaml, secrets.example.yaml
```

## Compétences couvertes (Bloc 1)

| Code | Compétence | Brique technique |
|------|-----------|-----------------|
| C1.1 | Base relationnelle | PostgreSQL 15 + PostGIS — schéma `ude`, star schema |
| C1.2 | Base non relationnelle | MongoDB 7 — Airparif live, TTL 24h |
| C1.3 | Data Lake sécurisé | HDFS — répertoires raw/silver partitionnés year/month/day |
| C1.4 | Scalabilité & résilience | Spark Standalone scale-out, Docker health checks, retry tenacity |
| C2.1 | API interopérable | FastAPI + Swagger, CORS, validation Pydantic v2 |
| C2.2 | Système streaming | Kafka — Airparif producer/consumer découplés |
| C2.3 | Transformation multi-sources | Spark ETL — 6 sources hétérogènes (CSV, API, Parquet, XLSX) |
| C2.4 | Pipelines optimisés | Spark cache/persist, JDBC batchsize, logs structlog, Spark UI |

## Documentation

| Doc | Contenu |
|-----|---------|
| [`docs/architecture.md`](docs/architecture.md) | Schéma global, justifications techniques, mapping compétences |
| [`docs/data_model.md`](docs/data_model.md) | Schéma SQL complet, structure MongoDB, star schema |
| [`docs/commands.md`](docs/commands.md) | Toutes les commandes PowerShell détaillées |
