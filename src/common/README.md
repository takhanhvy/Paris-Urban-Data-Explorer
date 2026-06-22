# src/common — Modules partagés

Code utilisé par tous les autres modules du projet.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `config.py` | Settings centralisés (pydantic-settings, chargement .env) |
| `logger.py` | Configuration du logging (YAML, structlog) |
| `utils.py` | Helpers partagés : `@log_pipeline_run`, `insee_to_arrondissement`, etc. |
| `exceptions.py` | Exceptions personnalisées (`IngestionError`, `APIError`, etc.) |

## Règle

Aucun module ici ne doit importer depuis `ingestion/`, `pipelines/` ou `src/api/`. Le flux d'import est unidirectionnel : tout le monde importe `src/common/`, `src/common/` n'importe personne.

## Techno

`pydantic-settings`, `python-yaml`, `structlog`
