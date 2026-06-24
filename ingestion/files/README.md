# ingestion/files — Feeders fichiers locaux

## Contenu actuel

Un seul script actif : `feeder_revenus.py`

## feeder_revenus.py

Convertit le fichier XLSX FiLoSoFi 2018 (`data/raw/BASE_TD_FILO_DEC_IRIS_2018.xlsx`) en CSV filtré sur Paris (`data/raw/revenus_iris_paris.csv`). La conversion passe par pandas car Spark ne lit pas nativement les fichiers `.xlsx` ; le CSV produit est ensuite lu par `pipelines/spark/feeder.py --source revenus`.

Opérations effectuées :
- Lecture XLSX (12 395 lignes, France entière) via `pandas.read_excel`
- Filtre sur `COM LIKE '75%'` (codes communes parisiennes)
- Export CSV UTF-8 dans `data/raw/`

## Exécution

```powershell
docker exec ude_api python ingestion/files/feeder_revenus.py
```

La commande est intégrée à `scripts/run_pipeline.ps1` (étape 2).

## Note

Les anciens scripts `feeder_dvf.py`, `feeder_delinquance.py` et `feeder_arrondissements.py` ont été supprimés : ces sources sont désormais lues directement par `pipelines/spark/feeder.py` depuis `data/raw/`.
