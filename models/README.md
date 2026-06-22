# models — Schémas dimensionnels

Documentation des modèles de données analytiques du projet.

## Fichiers

| Fichier | Description |
|---------|-------------|
| `star_schema.md` | Diagramme et requêtes du modèle dimensionnel (star schema) |

## Modèle principal

**Star schema** centré sur `ude.indicateurs_gold` (table de faits) avec 3 dimensions :
- `ude.arrondissements` — dimension géographique (avec géométrie PostGIS)
- Dimension temps (année : 2020–2025)
- Dimension type de bien (Appartement / Maison)

Ce modèle est directement utilisé par l'API FastAPI pour alimenter le dashboard.

Voir [`docs/data_model.md`](../docs/data_model.md) pour le schéma complet SQL + MongoDB + dimensionnel.
