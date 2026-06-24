# src/common — Modules partagés

Code transverse utilisé par les feeders API et l'application FastAPI.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `config.py` | Settings centralisés (pydantic-settings, chargement `.env`) |
| `logger.py` | Configuration du logging JSON (structlog + rotation) |
| `utils.py` | Décorateur `@log_pipeline_run` pour les feeders API |
| `exceptions.py` | Exceptions personnalisées (`IngestionError`, `APIError`, etc.) |

## Règle d'import

Flux unidirectionnel : tout le monde importe `src.common`, `src.common` n'importe personne d'autre. Pas de dépendance circulaire.

## Note sur les Spark jobs

Les jobs Spark (`pipelines/spark/`) n'importent pas `src.common` — ils sont autonomes et soumis via `spark-submit`. Ils utilisent `argparse` + variables d'environnement directement. Le décorateur `@log_pipeline_run` est réservé aux feeders Python classiques (`ingestion/api/`).

## Techno

`pydantic-settings`, `python-yaml`, `structlog`
