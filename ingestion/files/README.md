# ingestion/files — Feeders fichiers (CSV, Parquet, XLSX)

Modules d'ingestion depuis des fichiers statiques vers `data/bronze/`.

## Contenu

| Fichier | Source | Format entrée | Format sortie |
|---------|--------|---------------|---------------|
| `feeder_dvf.py` | DVF 2020–2025 | CSV | Parquet (1 fichier / an) |
| `feeder_delinquance.py` | INSEE délinquance | Parquet | Parquet (filtré Paris) |
| `feeder_revenus.py` | INSEE FiLoSoFi 2018 | XLSX | Parquet |

## Règle Bronze

Les feeders **ne transforment pas** les données (pas de calcul métier). Ils :
1. Lisent le fichier source depuis `data/raw/`
2. Filtrent si nécessaire (Paris uniquement)
3. Convertissent en Parquet (format colonnaire performant)
4. Écrivent dans `data/bronze/`

## Techno

`pandas`, `pyarrow`, `openpyxl`

## Dépendances

- `src/common/config.py` — chemins data
- `src/common/utils.py` — décorateur `@log_pipeline_run`

## Exécution

```bash
python -m ingestion.files.feeder_dvf
make ingest-batch   # tous les feeders
```
