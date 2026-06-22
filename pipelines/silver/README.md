# pipelines/silver — Processors Silver (C2.3)

Transformation Bronze → Silver : nettoyage, normalisation, filtrage Paris.

## Fichiers

| Fichier | Source | Sortie Silver |
|---------|--------|---------------|
| `processor_dvf.py` | `data/bronze/dvf_*.parquet` | `data/silver/transactions_paris.parquet` |
| `processor_logements_sociaux.py` | `data/bronze/logements_sociaux_raw.json` | `data/silver/logements_sociaux.parquet` |
| `processor_delinquance.py` | `data/bronze/delinquance_raw.parquet` | `data/silver/delinquance_paris.parquet` |
| `processor_revenus.py` | `data/bronze/revenus_iris_raw.parquet` | `data/silver/revenus_iris_paris.parquet` |

## Règles Silver (non-négociables)

- Filtrer sur Paris uniquement (codes INSEE 75101–75120)
- Tous les types numériques normalisés
- Toutes les coordonnées en WGS84 (EPSG:4326)
- Colonne `arrondissement` (int 1–20) présente dans chaque table
- Pas de doublons

## Techno

`pandas`, `geopandas`, `pyproj` (reprojection Lambert93 → WGS84)

## Dépendances

- Données Bronze présentes dans `data/bronze/`
- `src/common/utils.py` — helpers INSEE, décorateur pipeline_run
