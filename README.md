# Urban Data Explorer

Plateforme data complète pour analyser le marché immobilier parisien : prix/m², logements sociaux, délinquance, qualité de l'air et revenus par arrondissement — alimentée par un pipeline Spark distribué sur Data Lake MinIO.

## Architecture

**Pipeline batch (Spark)**

```
data/raw/          ── landing zone commune (CSV, Parquet, JSON, XLSX)
    │  feeder.py
    ▼
MinIO bronze       ── Parquet partitionné par date
    │  processor.py
    ▼
MinIO silver       ── nettoyé, typé, filtré Paris
    │  datamart.py (JDBC)
    ▼
PostgreSQL         ── ude.indicateurs_gold → FastAPI → Dashboard
```

**Pipeline streaming (Kafka) — KPI Qualité de l'air uniquement**

```
API Airparif (live)  →  airparif_producer.py  →  Kafka  →  airparif_consumer.py  →  MongoDB  →  FastAPI
```

> Le KPI "Qualité de l'air" est lu depuis **MongoDB** (chemin Kafka), pas depuis le pipeline Spark. Pour qu'il s'affiche, `airparif_producer.py` et `airparif_consumer.py` doivent tourner. Si MongoDB est vide, le KPI affiche `N/A`.

## Stack technique

| Couche | Techno |
|--------|--------|
| Compute distribué | Apache Spark 4.1.2 Standalone |
| Data Lake | MinIO S3-compatible (pattern Medallion bronze/silver) |
| Base relationnelle | PostgreSQL 15 + PostGIS — schéma `ude` |
| Base NoSQL | MongoDB 7 — Airparif live TTL 24h |
| Streaming | Apache Kafka — topic `airparif.quality` |
| API | FastAPI + pg8000 + SQLAlchemy 2.0 |
| Dashboard | JavaScript + MapLibre GL JS |
| Conteneurisation | Docker Compose (9 services) |

## Sources de données (6 types)

| # | Source | Format | Volume |
|---|--------|--------|--------|
| 1 | DVF — valeurs foncières 2020–2025 | CSV | ~395k lignes |
| 2 | Logements sociaux Paris | API REST opendata.paris.fr | 4 174 records |
| 3 | Délinquance INSEE | Parquet (filtré Paris) | — |
| 4 | Qualité de l'air Airparif | API live + Kafka | 20 arrondissements |
| 5 | Revenus FiLoSoFi INSEE 2018 | XLSX → CSV | 12 395 IRIS |
| 6 | Résidences principales INSEE 2022 | CSV (base-cc-logement-2022.CSV) | — |

## Prérequis

- Docker Desktop (avec au moins 4 Go RAM alloués)
- PowerShell (Windows)
- Fichiers sources dans `data/raw/` : `dvf_75_2020.csv` à `dvf_75_2025.csv`, `delinquance.parquet`, `BASE_TD_FILO_DEC_IRIS_2018.xlsx`, `base-cc-logement-2022.CSV`

## Démarrage rapide

```powershell
# 1. Configurer les variables d'environnement
Copy-Item .env.example .env

# 2. Lancer la stack complète (premier démarrage : build l'image API)
docker compose up -d --build

# 3. Vérifier que tous les services sont running
docker compose ps

# 4. Lancer le pipeline complet (DDL + feeders + Spark feeder + processor + datamart)
.\scripts\run_pipeline.ps1
```

## Interfaces

| Interface | URL |
|-----------|-----|
| Dashboard | http://localhost:8000/dashboard/ |
| API Swagger | http://localhost:8000/docs |
| Spark UI | http://localhost:8080 |
| MinIO console | http://localhost:9001 |

## Structure du projet

```
Paris-Urban-Data-Explorer/
├── data/raw/               Landing zone commune : CSV DVF, Parquet délinquance, XLSX revenus,
│                           JSON logements sociaux, JSON Airparif batch,
│                           CSV résidences principales INSEE 2022
├── pipelines/spark/        Jobs Spark Python (feeder, processor, datamart) + scripts submit/
├── ingestion/
│   ├── api/                Feeders API REST (logements sociaux, Airparif batch)
│   ├── files/              Conversion XLSX revenus → CSV
│   └── streaming/          Airparif → Kafka → MongoDB (temps réel)
├── src/
│   ├── api/                FastAPI — endpoints + dashboard HTML
│   └── common/             Config, logger, utils, exceptions partagés
├── sql/ddl/                Schéma PostgreSQL (00_init → 07_pipeline_runs)
├── dashboard/              Frontend MapLibre GL JS
├── scripts/                run_pipeline.ps1 — orchestrateur complet
├── docs/                   Architecture, data model, commandes
├── models/                 Documentation star schema
├── monitoring/             Logs applicatifs JSON (structlog)
├── orchestration/          Documentation orchestration (Airflow prévu Phase 2)
└── config/                 dev.yaml, logging.yaml, secrets.example.yaml
```

## Compétences couvertes (Bloc 1)

| Code | Compétence | Brique technique |
|------|-----------|-----------------|
| C1.1 | Base relationnelle | PostgreSQL 15 + PostGIS — schéma `ude`, star schema |
| C1.2 | Base non relationnelle | MongoDB 7 — Airparif live, TTL 24h |
| C1.3 | Data Lake sécurisé | MinIO — Parquet Bronze/Silver partitionné par date |
| C1.4 | Scalabilité & résilience | Spark Standalone scale-out, Docker health checks, retry tenacity |
| C2.1 | API interopérable | FastAPI + Swagger, auth X-API-Key, validation Pydantic v2 |
| C2.2 | Système streaming | Kafka — Airparif producer/consumer découplés |
| C2.3 | Transformation multi-sources | PySpark — 5 sources hétérogènes (CSV, Parquet, XLSX, JSON, API) |
| C2.4 | Pipelines optimisés | cache/persist Spark, partitionnement Parquet, Spark UI, batchsize JDBC |

## Documentation

| Doc | Contenu |
|-----|---------|
| [`docs/architecture.md`](docs/architecture.md) | Diagramme Mermaid, mapping compétences, optimisations Spark |
| [`docs/data_model.md`](docs/data_model.md) | Schéma SQL complet, structure MongoDB, silver Parquet |
| [`docs/commands.md`](docs/commands.md) | Toutes les commandes PowerShell |
