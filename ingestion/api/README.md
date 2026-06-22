# ingestion/api — Feeders API REST

Modules d'ingestion depuis des APIs REST externes vers `data/bronze/`.

## Contenu

| Fichier | Source | Volume |
|---------|--------|--------|
| `feeder_logements_sociaux.py` | opendata.paris.fr | 4 174 enregistrements |

## Comportement

- Pagination automatique (100 records / page)
- Retry avec backoff exponentiel via `tenacity` (3 tentatives max)
- Sortie : JSON brut dans `data/bronze/`

## Techno

`requests`, `tenacity`

## Dépendances

- `src/common/config.py` — URL et paramètres API
- `src/common/utils.py` — décorateur `@log_pipeline_run`

## Exécution

```bash
python -m ingestion.api.feeder_logements_sociaux
# ou
make ingest-batch
```
