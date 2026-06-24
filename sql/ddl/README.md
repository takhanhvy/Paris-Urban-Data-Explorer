# sql/ddl — Schéma relationnel PostgreSQL (C1.1)

Scripts DDL du schéma `ude` sur PostgreSQL 15 + PostGIS 3.4. Ils sont exécutés automatiquement au premier démarrage du conteneur via `docker-entrypoint-initdb.d/`. Tous les scripts sont idempotents (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`).

## Fichiers et ordre d'exécution

| Ordre | Fichier | Table créée | Description |
|-------|---------|-------------|-------------|
| 00 | `00_init.sql` | — | Active l'extension PostGIS, crée le schéma `ude` |
| 01 | `01_arrondissements.sql` | `ude.arrondissements` | Dimension géographique : 20 arrondissements, géométrie PostGIS WGS84, superficie, population, densité |
| 02 | `02_transactions_dvf.sql` | `ude.transactions_dvf` | Transactions immobilières DVF filtrées Paris, index géospatial sur `geom` |
| 03 | `03_logements_sociaux.sql` | `ude.logements_sociaux` | Logements sociaux financés (volumes PLAI/PLUS/PLS par arrondissement et année) |
| 04 | `04_delinquance.sql` | `ude.delinquance` | Faits de délinquance par arrondissement × année × indicateur, taux pour mille |
| 05 | `05_revenus_iris.sql` | `ude.revenus_iris` | Revenus FiLoSoFi 2018 à la maille IRIS (déciles, Gini, revenu médian) |
| 06 | `06_indicateurs_gold.sql` | `ude.indicateurs_gold` | Table de faits Gold — agrégats prêts pour l'API, alimentée exclusivement par Spark `datamart.py` via JDBC |
| 07 | `07_pipeline_runs.sql` | `ude.pipeline_runs` | Historique des runs de pipeline (monitoring C2.4) |

## Notes

- `ude.indicateurs_gold` est alimentée par `pipelines/spark/datamart.py` en mode `overwrite` via JDBC. Ne pas y insérer manuellement.
- `ude.arrondissements.densite_population` est calculée par `processor.py` (`population_totale / superficie_km2`) et stockée ici — l'API la lit directement.
- `ude.pipeline_runs` est remplie par le décorateur `@log_pipeline_run` de `src/common/utils.py` (feeders Python).

## Exécution manuelle (si le volume PostgreSQL est recréé)

```powershell
foreach ($f in "00_init","01_arrondissements","02_transactions_dvf","03_logements_sociaux","04_delinquance","05_revenus_iris","06_indicateurs_gold","07_pipeline_runs") {
    Get-Content "sql\ddl\$f.sql" | docker exec -i ude_postgres psql -U ude_user -d urban_data
}
```
