# orchestration — Orchestration des pipelines (C2.4)

Planification et enchaînement des jobs Spark.

## Exécution actuelle (spark-submit)

Le pipeline complet est déclenché manuellement via les scripts dans `pipelines/spark/submit/` :

```powershell
# Étape 1 — Feeder : sources locales → MinIO /raw
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh all

# Étape 2 — Processor : MinIO /raw → MinIO /silver
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh all

# Étape 3 — Datamart : MinIO /silver → PostgreSQL
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh
```

Les scripts lisent leur configuration depuis les variables d'environnement du conteneur (MinIO endpoint, credentials Spark, JDBC URL) — aucun chemin codé en dur.

## Fréquences recommandées

| Job | Fréquence | Déclencheur |
|-----|-----------|-------------|
| `feeder.sh logements_sociaux` | Mensuel | Mise à jour API Paris OpenData |
| `feeder.sh dvf` | Annuel | Publication DVF (juillet N+1) |
| `feeder.sh all` + `processor.sh all` + `datamart.sh` | À la demande | Rechargement complet |

## Phase 2 — Orchestrateur cible

Remplacement des scripts manuels par un orchestrateur (Prefect ou Airflow) avec :
- DAG `batch_dvf_annual` : feeder DVF → processor → datamart
- DAG `batch_logements_monthly` : feeder logements sociaux → processor → datamart
- Alertes sur échec, retry automatique
- Visibilité des runs dans l'UI Prefect / Airflow

## Techno actuelle

Scripts `bash` + `spark-submit` + Docker Compose
