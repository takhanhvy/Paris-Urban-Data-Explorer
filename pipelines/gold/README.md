# pipelines/gold — Processor Gold

Agrège les données Silver pour alimenter `ude.indicateurs_gold` (PostgreSQL) et les collections MongoDB.

## Responsabilités

- Jointure des 5 sources Silver sur `arrondissement × annee`
- Calcul des agrégats : prix médian/m², taux logements sociaux, score attractivité
- Chargement dans PostgreSQL (`ude.indicateurs_gold`) via SQLAlchemy
- Mise à jour des collections MongoDB (shapes GeoJSON)
- Enregistrement dans `ude.pipeline_runs`

## Fichiers (à créer en Phase 2)

| Fichier | Rôle |
|---------|------|
| `processor_gold.py` | Jointure + agrégation Silver → Gold |
| `loader_postgres.py` | UPSERT dans ude.indicateurs_gold |
| `loader_mongo.py` | Update collections MongoDB |

## Techno

`pandas`, `sqlalchemy`, `pymongo`

## Dépendances

- Données Silver dans `data/silver/`
- Services PostgreSQL + MongoDB opérationnels
