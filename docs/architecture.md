# Architecture — Urban Data Explorer

## Schéma global

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCES BRUTES                                                 │
│  DVF CSV │ API logements │ Parquet délinquance │ XLSX revenus   │
│                          │ API Airparif (live)                  │
└────────────────┬─────────┴──────────────┬──────────────────────┘
                 │                        │ streaming
                 ▼                        ▼
        ingestion/api/           ingestion/streaming/
        feeder_logements         airparif_producer.py
        _sociaux.py              → Kafka topic airparif.quality
        → data/bronze/           → airparif_consumer.py
          logements_sociaux      → MongoDB air_quality (TTL 24h)
          _raw.json
                 │
                 ▼
         data/raw/  (CSV, Parquet, XLSX, JSON — montés en volume Docker)
                 │
                 │  spark-submit feeder.py --source all
                 ▼
    MinIO  s3a://urban-data/raw/
           ├── dvf/ingestion_year=2026/ingestion_month=06/ingestion_day=22/
           ├── delinquance/ingestion_year=2026/ingestion_month=06/ingestion_day=22/
           ├── logements_sociaux/ingestion_year=2026/ingestion_month=06/ingestion_day=22/
           └── revenus/ingestion_year=2026/ingestion_month=06/ingestion_day=22/
                 │  cache() → visible Spark UI Storage
                 │
                 │  spark-submit processor.py --source all
                 ▼
    MinIO  s3a://urban-data/silver/
           ├── transactions/ingestion_year=.../ingestion_month=.../ingestion_day=.../
           ├── delinquance/
           ├── logements_sociaux/
           └── revenus/
                 │  persist(MEMORY_AND_DISK) → visible Spark UI Storage
                 │
                 │  spark-submit datamart.py (JDBC)
                 ▼
    PostgreSQL   ude.indicateurs_gold   (arrondissement × annee)
                 │  cache() sur chaque source silver
                 │
                 ▼
    FastAPI  src/api/         ←──── MongoDB air_quality (TTL 24h)
                 │
                 ▼
    Dashboard  MapLibre GL JS (http://localhost:8000/dashboard)
```

---

## Services Docker Compose

| Conteneur | Image | Ports | Rôle |
|-----------|-------|-------|------|
| ude_postgres | postgis/postgis:15-3.4 | 5433 | Datamarts Gold, star schema |
| ude_mongodb | mongo:7.0 | 27017 | Airparif live, TTL 24h |
| ude_zookeeper | confluentinc/cp-zookeeper:7.6.1 | — | Coordination Kafka |
| ude_kafka | confluentinc/cp-kafka:7.6.1 | 29092 (externe) | Streaming Airparif |
| ude_minio | minio/minio | 9000 (API), 9001 (console) | Data Lake S3-compatible |
| ude_minio_init | minio/mc | — | Création buckets au démarrage |
| ude_spark_master | bitnami/spark:3.5 | 8080 (UI), 7077 (RPC) | Orchestration Spark |
| ude_spark_worker | bitnami/spark:3.5 | — | Exécution (2 CPU, 2 Go RAM) |
| ude_api | Dockerfile custom | 8000 | FastAPI + dashboard statique |

---

## Data Lake MinIO — structure des buckets

MinIO expose une API S3 standard. Spark lit/écrit via le connecteur `S3A` :

```
spark.hadoop.fs.s3a.endpoint           = http://minio:9000
spark.hadoop.fs.s3a.path.style.access  = true
spark.hadoop.fs.s3a.impl               = org.apache.hadoop.fs.s3a.S3AFileSystem
```

```
urban-data/
├── raw/
│   ├── dvf/
│   │   └── ingestion_year=2026/ingestion_month=06/ingestion_day=22/
│   │       └── part-00000-*.parquet
│   ├── delinquance/
│   ├── logements_sociaux/
│   └── revenus/
└── silver/
    ├── transactions/      ← DVF filtre Paris, prix_m2 calculé, types castés
    ├── delinquance/       ← filtré 75101–75120, arrondissement extrait
    ├── logements_sociaux/ ← types normalisés, arrondissement 1–20
    └── revenus/           ← filtré Paris, déciles en double
```

Le partitionnement `ingestion_year/ingestion_month/ingestion_day` active le predicate pushdown Parquet : `processor.py` lit uniquement la partition cible sans scanner tout le bucket.

---

## Les 3 jobs Spark

### feeder.py — Raw sources → MinIO /raw

- Paramètres : `--source`, `--input-path`, `--output-path`, `--date` (aucun chemin codé en dur)
- DVF : lecture multi-CSV en un DataFrame, `cache()` avant `count()` de validation + écriture
- Revenus XLSX : pont pandas → Spark (`spark.createDataFrame(pd.read_excel(...))`)
- Logements sociaux : lecture JSON avec fallback `data/bronze/` si staging vide

### processor.py — MinIO /raw → MinIO /silver

- DVF : filtre `code_commune LIKE '751%'`, cast types, calcul `prix_m2`, filtre aberrants (500–80 000 €/m²)
- Délinquance : filtre `CODGEO_2025 IN 75101..75120`, extraction arrondissement
- Logements sociaux : normalisation `arrdt → arrondissement`, cast volumes
- Revenus : filtre `COM LIKE '75%'`, cast déciles en double
- `persist(MEMORY_AND_DISK)` après nettoyage : réutilisé pour stats qualité + écriture

### datamart.py — MinIO /silver → PostgreSQL

- `cache()` sur chaque source silver (réutilisé pour groupBy multiples)
- Agrégation DVF : `percentile_approx` pour médiane/Q1/Q3, `avg` pour prix moyen
- Score attractivité : normalisation Min-Max via window function `partitionBy("annee")`
- Écriture JDBC PostgreSQL (`org.postgresql:postgresql:42.7.3`, batchsize 10 000)

---

## Ingestion temps réel — Airparif (C2.2)

Deux variantes coexistent :

**Batch** — `ingestion/api/feeder_airparif_batch.py` : 20 appels async aiohttp → `replace_one upsert` MongoDB. À lancer manuellement ou via cron.

**Streaming** — `ingestion/streaming/` : `airparif_producer.py` produit dans le topic Kafka `airparif.quality` toutes les heures. `airparif_consumer.py` consomme et écrit dans MongoDB. Le découplage Kafka permet de rejouer les messages et d'ajouter des consumers (alertes, archivage) sans toucher au producer.

La collection MongoDB `air_quality` a un index TTL de 24h sur `inserted_at` : expiration automatique sans purge manuelle.

---

## Optimisations Spark (C2.4)

| Optimisation | Où | Effet visible dans Spark UI |
|---|---|---|
| `cache()` | feeder.py, datamart.py sur sources silver | Storage tab : DataFrame mis en cache |
| `persist(MEMORY_AND_DISK)` | processor.py après nettoyage | Storage tab : spillover disque si RAM insuffisante |
| `spark.sql.shuffle.partitions=8` | Tous les scripts submit/ | Stages tab : 8 partitions au lieu de 200 après groupBy |
| Partitionnement Parquet date | feeder.py + processor.py | Predicate pushdown — lecture partition cible uniquement |
| `--name` job | Tous les scripts submit/ | Jobs tab : noms lisibles par type de job |

---

## Mapping compétences ↔ briques (synthèse soutenance)

| Code | Brique | Techno | Argument (1 phrase) |
|------|--------|--------|---------------------|
| C1.1 | `ude.indicateurs_gold` + tables dim | PostgreSQL 15 + PostGIS | Schéma normalisé, FK, index géospatiaux, alimenté par JDBC Spark |
| C1.2 | Collection `air_quality` TTL 24h | MongoDB 7 | Documents JSON imbriqués, expiration auto, requêtes par code INSEE |
| C1.3 | Buckets `raw/` + `silver/` partitionnés | MinIO S3-compatible | Data Lake structuré, partitionnement date, accès S3A identique à AWS en prod |
| C1.4 | Spark Standalone scale-out + Docker health checks + tenacity retry | Spark 3.5, Docker | Workers ajoutables sans modifier le code, auto-retry APIs, déploiement en une commande |
| C2.1 | 7 endpoints dashboard + auth + OpenAPI | FastAPI + Pydantic v2 | Auth X-API-Key, validation stricte des paramètres, Swagger auto sur `/docs` |
| C2.2 | Producer → Kafka → consumer → MongoDB | Kafka 7.6 + asyncio | 20 appels Airparif async, découplage total, replay possible depuis le topic |
| C2.3 | `processor.py` — 4 sources → silver unifié | PySpark 3.5 | CSV + Parquet + XLSX + JSON normalisés et joinables sur `arrondissement × annee` |
| C2.4 | 3 jobs spark-submit + cache/persist + partitionnement | PySpark 3.5 | Aucun chemin codé, `cache()` visible Spark UI Storage, shuffle partitions optimisé |
