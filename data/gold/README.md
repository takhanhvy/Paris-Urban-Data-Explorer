# data/gold — Couche Gold

Données agrégées, prêtes pour l'API et le dashboard. La couche Gold vit principalement en **base de données** (PostgreSQL + MongoDB), pas en fichiers.

## Où vivent les données Gold ?

| Données | Stockage | Table / Collection |
|---------|----------|--------------------|
| Indicateurs par arrondissement × année | PostgreSQL | `ude.indicateurs_gold` |
| Arrondissements + polygones GeoJSON | PostgreSQL | `ude.arrondissements` |
| Cache qualité de l'air | MongoDB | `air_quality` (TTL 24h) |
| Shapes GeoJSON dashboard | MongoDB | `arrondissement_shapes` |

## Ce dossier

Utilisé pour des exports ponctuels (ex. export CSV pour la soutenance, fichier GeoJSON des arrondissements). En production, les données sont directement lues depuis PostgreSQL/MongoDB par l'API.

## Alimenté par

`pipelines/gold/`
