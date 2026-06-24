# src/common — Modules partagés

Code transverse utilisé par les feeders API (`ingestion/api/`) et l'application FastAPI (`src/api/`).

## Fichiers

| Fichier | Rôle |
|---------|------|
| `config.py` | `Settings` Pydantic-settings : charge `.env`, expose les URLs PostgreSQL, MongoDB, Kafka, la clé API Airparif et les paramètres CORS. Singleton via `@lru_cache` (`get_settings()`) |
| `logger.py` | Configuration structlog JSON avec rotation (10 MB, 5 backups). Sortie dans `monitoring/logs/ude.log`. Fonction `get_logger(__name__)` |
| `utils.py` | Décorateur `@log_pipeline_run` pour les feeders Python : loggue `pipeline_name`, `source`, `layer`, `started_at`, `duration_s`, `statut` |
| `exceptions.py` | Exceptions personnalisées : `IngestionError`, `APIError`, `ConfigError` |

## Règle d'import

Flux unidirectionnel : `ingestion/` et `src/api/` importent `src.common`, mais `src.common` n'importe aucun autre module du projet. Pas de dépendance circulaire.

## Note sur les jobs Spark

`pipelines/spark/feeder.py`, `processor.py` et `datamart.py` n'importent pas `src.common` : ils sont soumis via `spark-submit` dans un contexte isolé et lisent leur configuration via `argparse` et variables d'environnement du conteneur Spark.

## Techno

`pydantic-settings`, `structlog`, `python-dotenv`
