# models — Modèle dimensionnel

Documentation du star schema analytique du projet. Le modèle est implémenté directement en SQL (fichiers `sql/ddl/`), sans dbt.

## Star schema

**Table de faits :** `ude.indicateurs_gold`
Clé composite : `(arrondissement, annee)`. Alimentée exclusivement par `pipelines/spark/datamart.py` via JDBC.

**Dimensions :**
- `ude.arrondissements` — dimension géographique : géométrie PostGIS, superficie, population, densité
- `annee` — dimension temps intégrée dans la table de faits (2020–2025)

**Métriques stockées dans `indicateurs_gold` :**

| Colonne | Source d'origine | Description |
|---------|-----------------|-------------|
| `prix_m2_median` | DVF | Médiane (`percentile_approx` Spark), valeur principale dashboard |
| `prix_m2_moyen` | DVF | Moyenne pondérée |
| `prix_m2_q1`, `prix_m2_q3` | DVF | Quartiles pour la distribution |
| `nb_transactions` | DVF | Nombre de ventes valides sur l'année |
| `surface_mediane` | DVF | Surface habitable médiane |
| `part_appartements` | DVF | Part des appartements dans les transactions (%) |
| `nb_logements_sociaux` | Logements sociaux API | Total livrés par arrondissement × année |
| `nb_plai`, `nb_plus_pluscd`, `nb_pls` | Logements sociaux API | Détail par type de financement |
| `taux_cambriolages_pmille` | Délinquance parquet | Taux pour mille habitants |
| `taux_violences_pmille` | Délinquance parquet | Taux pour mille habitants |
| `taux_delinquance_global` | Délinquance parquet | Indicateur synthétique |
| `revenu_median_arr` | FiLoSoFi IRIS 2018 | Revenu médian par UC, agrégé depuis la maille IRIS |
| `indice_gini_arr` | FiLoSoFi IRIS 2018 | Indice de Gini (inégalités intra-arrondissement) |
| `score_attractivite` | Calculé par Spark | Score 0–100 Min-Max normalisé |

## Score attractivité

Calculé dans `datamart.py` par normalisation Min-Max sur fenêtre annuelle (`Window.partitionBy("annee")`). Formule :

```
score = revenu_norm × 35 + (1 - delinquance_norm) × 30 + (1 - prix_norm) × 25 + logements_norm × 10
```

## Requêtes typiques

```sql
-- Carte choroplèthe 2024
SELECT arrondissement, prix_m2_median, score_attractivite
FROM ude.indicateurs_gold WHERE annee = 2024;

-- Timeline 7e arrondissement
SELECT annee, prix_m2_median FROM ude.indicateurs_gold
WHERE arrondissement = 7 ORDER BY annee;
```

Voir `docs/data_model.md` pour le schéma complet de toutes les tables.
