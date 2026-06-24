# monitoring — Métriques et logs

Suivi des performances des pipelines et de la santé des services.

## Structure

```
monitoring/
└── logs/          → ude.log (JSON rotatif, config dans config/logging.yaml)
```

## Logs applicatifs

Les logs JSON sont écrits dans `monitoring/logs/ude.log` (rotatif 10 MB, 5 backups) par les feeders API (`ingestion/api/`). Format JSON pour intégration avec des outils d'analyse (Loki, ELK, etc.).

Le décorateur `@log_pipeline_run` (dans `src/common/utils.py`) loggue automatiquement pour chaque run :
- `pipeline_name`, `source`, `layer`
- `started_at`, `duration_s`
- `statut` : `success` / `failed`
- Erreur complète en cas d'échec

## Performances Spark

Les jobs Spark (`feeder.py`, `processor.py`, `datamart.py`) loggent directement dans stdout et sont visibles dans :
- **Spark UI** → `http://localhost:8080` : jobs, stages, durées, cache Storage
- Logs Docker : `docker compose logs -f ude_spark_master`

Exemple de métriques affichées par le Spark datamart :
```
[datamart] Silver/transactions : 523 847 lignes (cache activé)
[datamart] ✓ 100 lignes écrites dans ude.indicateurs_gold (mode=overwrite)
[datamart] Pipeline terminé.
```

## Table ude.pipeline_runs

La DDL `sql/ddl/07_pipeline_runs.sql` crée la table de monitoring PostgreSQL. L'alimentation automatique depuis `@log_pipeline_run` de \src\common\utils.py.

## Techno

`structlog`, `logging` (stdlib)
