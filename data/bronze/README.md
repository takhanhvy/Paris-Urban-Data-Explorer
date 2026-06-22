# data/bronze — Couche Bronze (C1.3)

Données brutes, fidèles aux sources originales. **Aucune transformation métier ici.**

## Contenu attendu

| Fichier | Source | Fréquence de mise à jour |
|---------|--------|--------------------------|
| `dvf_2020.parquet` … `dvf_2025.parquet` | DVF CSV | Annuel |
| `logements_sociaux_raw.json` | API opendata.paris.fr | Mensuel |
| `delinquance_raw.parquet` | INSEE Parquet | Annuel |
| `revenus_iris_raw.parquet` | XLSX FiLoSoFi | Unique (2018) |
| `airparif/YYYY-MM-DD_HH.json` | API Airparif live | Archivage optionnel |

## Règles

- Les fichiers Bronze sont **immutables** : on ne les modifie jamais, on les recrée depuis `data/raw/`
- Le Bronze est dans `.gitignore` : les données ne sont pas versionnées
- Tout pipeline Silver peut être rejoué depuis le Bronze

## Alimenté par

`ingestion/files/`, `ingestion/api/`
