# orchestration — Orchestration des pipelines

## Orchestrateur actuel : scripts/run_pipeline.ps1

Le pipeline complet est déclenché via le script PowerShell `scripts/run_pipeline.ps1`. Il enchaîne 5 étapes dans l'ordre :

1. **DDL PostgreSQL** — applique les 8 scripts `sql/ddl/` de façon idempotente
2. **Feeders Python** — `feeder_logements_sociaux`, `feeder_airparif_batch`, `feeder_revenus` (dans `ude_api`)
3. **Spark feeder** — `data/raw/` → MinIO `s3a://urban-data/bronze/` (toutes les sources)
4. **Spark processor** — MinIO bronze → MinIO `s3a://urban-data/silver/`
5. **Spark datamart** — MinIO silver → PostgreSQL `ude.indicateurs_gold`

```powershell
# Lancer le pipeline complet
.\scripts\run_pipeline.ps1
```

Le script vérifie que les 4 conteneurs critiques (`ude_postgres`, `ude_api`, `ude_minio`, `ude_spark_master`) sont `running` avant de démarrer.

## Jobs Spark individuels

Les scripts `pipelines/spark/submit/` permettent de relancer une étape ou une source spécifique :

```powershell
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh dvf 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh dvf 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024
```

## Phase 2 — Airflow (prévu, non implémenté)

Le dossier `orchestration/airflow/dags/` accueillera les DAGs Airflow lorsque l'équipe migrera vers une orchestration automatisée. DAGs prévus :

- `batch_dvf_annual` : feeder DVF → processor → datamart (1 fois/an, publication juillet)
- `batch_logements_monthly` : feeder logements sociaux → processor → datamart (mensuel)
- `streaming_airparif` : producer/consumer Kafka (horaire)

Jusqu'à cette migration, `run_pipeline.ps1` est la référence pour l'exécution.
