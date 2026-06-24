# monitoring — Métriques et logs (C2.4)

Suivi de la santé des services et des performances des pipelines.

## Structure

```
monitoring/
└── logs/
    └── ude.log    ← logs JSON rotatifs (10 MB, 5 backups)
```

## Logs applicatifs

Les feeders Python (`ingestion/api/`) écrivent des logs JSON dans `monitoring/logs/ude.log` via structlog. Configuration dans `config/logging.yaml`.

Le décorateur `@log_pipeline_run` (`src/common/utils.py`) enregistre automatiquement pour chaque run :

- `pipeline_name`, `source`, `layer`
- `started_at`, `duration_s`
- `statut` : `success` / `failed`
- traceback complet en cas d'échec

Les runs sont aussi persistés dans `ude.pipeline_runs` (PostgreSQL) pour consultation SQL.

## Performances Spark

Les jobs Spark (`feeder.py`, `processor.py`, `datamart.py`) loggent dans stdout et sont visibles via :

- **Spark UI** : `http://localhost:8080` — onglets Jobs, Stages, Storage (cache/persist), SQL
- Logs Docker : `docker compose logs -f ude_spark_master`

## Health check API

```powershell
Invoke-RestMethod "http://localhost:8000/health"
```

Retourne `{"status": "ok"}` si l'API et les connexions BDD sont opérationnelles.

## Interfaces de monitoring

| Interface | URL | Description |
|-----------|-----|-------------|
| Spark UI | http://localhost:8080 | Jobs, stages, cache Storage, exécuteurs |
| API health | http://localhost:8000/health | Statut API + connexions |
| MinIO console | http://localhost:9001 | Exploration buckets bronze/silver |
