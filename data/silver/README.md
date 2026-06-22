# data/silver — Couche Silver (C2.3)

Données nettoyées, normalisées, filtrées sur Paris, prêtes pour l'agrégation Gold.

## Contenu attendu

| Fichier | Issu de | Transformations clés |
|---------|---------|---------------------|
| `transactions_paris.parquet` | DVF Bronze | Filtre Paris, calcul prix/m², normalisation types |
| `logements_sociaux.parquet` | JSON Bronze | Lambert93 → WGS84, standardisation arrdt |
| `delinquance_paris.parquet` | Parquet Bronze | Filtre 75101–75120, extraction arrondissement |
| `revenus_iris_paris.parquet` | Parquet Bronze | Filtre Paris, sélection 12 colonnes |
| `qualite_air_current.json` | Kafka / MongoDB | Agrégat des 20 arrondissements |

## Garanties Silver

- Colonne `arrondissement` (int 1–20) présente dans chaque fichier
- Coordonnées en WGS84 (EPSG:4326) uniquement
- Types Pandas corrects (dates, numériques, strings)
- Pas de doublons
- Fichiers Parquet compressés (snappy)

## Alimenté par

`pipelines/silver/`
