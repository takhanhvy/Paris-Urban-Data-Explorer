# models — Schémas dimensionnels

Documentation du modèle de données analytique du projet.

## Modèle principal — Star schema

**Table de faits :** `ude.indicateurs_gold` (alimentée par Spark datamart.py)

**Dimensions :**
- `ude.arrondissements` — dimension géographique (géométrie PostGIS, superficie, population, densité)
- Dimension temps — colonne `annee` (2020–2025)

**Métriques dans la table de faits :**

| Colonne | Source | Description |
|---------|--------|-------------|
| `prix_m2_median` | DVF | Prix médian au m² (percentile_approx Spark) |
| `prix_m2_moyen` | DVF | Prix moyen au m² |
| `nb_transactions` | DVF | Nombre de ventes |
| `nb_logements_sociaux` | Logements sociaux API | Total logements sociaux livrés |
| `taux_delinquance_global` | Délinquance parquet | Taux pour mille habitants |
| `revenu_median_arr` | Revenus FiLoSoFi | Revenu médian par UC (2018, IRIS → arrondissement) |
| `score_attractivite` | Calculé | Score 0–100 Min-Max (Spark datamart.py) |

## Score attractivité

Formule Min-Max normalisée (Window Spark, partitionnée par année) :

```
score = revenu_norm × 35
      + (1 - delinquance_norm) × 30
      + (1 - prix_norm) × 25
      + logements_norm × 10
```

## Référence complète

Voir [`docs/data_model.md`](../docs/data_model.md) — schéma SQL complet, structure MongoDB, et requêtes analytiques.
