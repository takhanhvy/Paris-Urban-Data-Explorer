# pipelines/bronze — Chargement Bronze → BDD

Pipelines qui chargent les données de `data/bronze/` dans des tables de staging PostgreSQL si nécessaire.

> **Note :** dans l'architecture actuelle, la plupart des données transitent directement Bronze → Silver (fichiers Parquet) sans passer par une table PostgreSQL intermédiaire. Ce module sera utilisé en Phase 2 si un staging SQL est nécessaire pour des jointures complexes.

## Techno

`sqlalchemy`, `psycopg2`

## Dépendances

- Service PostgreSQL opérationnel
- `src/common/config.py` — connexion PostgreSQL
