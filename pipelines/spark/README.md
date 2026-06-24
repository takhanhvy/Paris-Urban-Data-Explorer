# pipelines/spark — Jobs Spark (C2.3, C2.4)

Trois jobs PySpark qui implémentent le pattern Medallion complet : ingestion depuis `data/raw/`, nettoyage vers MinIO silver, agrégation vers PostgreSQL.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `feeder.py` | Lit les sources locales (`data/raw/`) → écrit en Parquet partitionné dans MinIO `s3a://urban-data/bronze/` |
| `processor.py` | Lit bronze MinIO → nettoie, caste, filtre Paris → écrit dans MinIO `s3a://urban-data/silver/` |
| `datamart.py` | Lit silver MinIO → agrège, calcule le score attractivité → écrit dans PostgreSQL `ude.indicateurs_gold` via JDBC |
| `submit/feeder.sh` | Script `spark-submit` pour feeder.py — paramètres source, date, chemins |
| `submit/processor.sh` | Script `spark-submit` pour processor.py |
| `submit/datamart.sh` | Script `spark-submit` pour datamart.py |

## Commandes

```powershell
# Feeder : toutes les sources
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh all

# Feeder : source spécifique avec date
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh dvf 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh logements_sociaux 2026-06-24

# Processor : toutes les sources
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh all

# Processor : source spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh dvf 2026-06-24

# Datamart : toutes les années
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh

# Datamart : année spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024
```

## Optimisations (C2.4)

- `cache()` dans feeder.py et datamart.py : les DataFrames sources sont mis en cache pour validation + écriture sans double lecture
- `persist(MEMORY_AND_DISK)` dans processor.py : après nettoyage, réutilisé pour stats qualité + écriture silver
- `spark.sql.shuffle.partitions=8` dans les scripts submit/ : réduit de 200 à 8 les partitions shuffle, adapté au volume Paris
- Partitionnement Parquet `ingestion_year/ingestion_month/ingestion_day` : predicate pushdown activé lors de la lecture en processor.py
- JDBC `batchsize=10000` dans datamart.py : écriture PostgreSQL groupée

Les métriques sont visibles dans Spark UI : `http://localhost:8080`

## Dépendances

- MinIO doit être `healthy` avant toute exécution (`ude_minio` + `ude_minio_init`)
- PostgreSQL doit être accessible sur `ude_postgres:5432` pour datamart.py
- Les fichiers sources DVF, délinquance, revenus doivent être présents dans `data/raw/`
- Les feeders Python (`ingestion/api/`) doivent avoir déposé `data/bronze/logements_sociaux_raw.json` avant le feeder Spark
