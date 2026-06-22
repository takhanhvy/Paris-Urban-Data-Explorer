# Urban Data Explorer 🏙️

Plateforme data complète pour analyser le marché immobilier parisien : prix/m², logements sociaux, délinquance, qualité de l'air et revenus par arrondissement.

## Stack technique

| Couche | Techno |
|--------|--------|
| Ingestion | Python (requests, asyncio, pandas, pyarrow) |
| Data Lake | Fichiers locaux Bronze / Silver / Gold |
| Base relationnelle | PostgreSQL 15 + PostGIS |
| Base NoSQL | MongoDB 7 |
| Streaming | Apache Kafka + Python producer/consumer |
| Orchestration | Apache Airflow 2 |
| API | FastAPI + Pydantic |
| Dashboard | JavaScript + MapLibre GL |
| Conteneurisation | Docker + Docker Compose |

## Sources de données

1. **DVF** — Valeurs foncières (CSV, 2020–2025)
2. **Logements sociaux** — opendata.paris.fr (API REST)
3. **Délinquance** — INSEE (Parquet)
4. **Qualité de l'air** — Airparif (API live, temps réel)
5. **Revenus FiLoSoFi** — INSEE 2018 (XLSX)

## Démarrage rapide (PowerShell / Windows)

```powershell
# 1. Copier les variables d'environnement
Copy-Item .env.example .env

# 2. Lancer la stack Docker (PostgreSQL, MongoDB, Kafka, API)
docker-compose up -d

# 3. Initialiser la base PostgreSQL (scripts dans l'ordre)
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/00_init.sql
# ... voir docs/commands.md pour les scripts suivants

# 4. Lancer l'ingestion initiale (batch)
python -m ingestion.files.feeder_dvf
python -m ingestion.api.feeder_logements_sociaux

# 5. Lancer l'API
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

> Toutes les commandes détaillées sont dans [`docs/commands.md`](docs/commands.md).

## Architecture

Voir [`docs/architecture.md`](docs/architecture.md) pour le schéma complet et les choix techniques.

## Structure du projet

```
data/           → Données par couche (raw → bronze → silver → gold)
ingestion/      → Feeders : collecte depuis les sources
pipelines/      → Processors par couche Medallion
sql/            → DDL, vues, fonctions PostgreSQL
src/            → Code partagé (common, api, processing)
models/         → Schémas dimensionnels
orchestration/  → DAGs Airflow
monitoring/     → Métriques, logs
docs/           → Documentation technique
config/         → Configuration par environnement
```

## Compétences couvertes (Bloc 1)

| Code | Compétence | Brique |
|------|-----------|--------|
| C1.1 | Base relationnelle | PostgreSQL + PostGIS |
| C1.2 | Base non relationnelle | MongoDB (cache air + GeoJSON) |
| C1.3 | Data Lake sécurisé | Bronze / Silver / Gold |
| C1.4 | Scalabilité & résilience | Docker Compose + retry |
| C2.1 | API interopérable | FastAPI + OpenAPI |
| C2.2 | Streaming distribué | Kafka (Airparif live) |
| C2.3 | Transformation multi-sources | ETL 5 sources → Silver |
| C2.4 | Pipelines optimisés | Airflow + métriques pipeline_runs |
