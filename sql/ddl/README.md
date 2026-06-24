# sql/ddl — Schéma relationnel PostgreSQL (C1.1)

Scripts DDL exécutés automatiquement au premier démarrage via `docker-entrypoint-initdb.d/`.

## Fichiers

| Fichier | Table | Description |
|---------|-------|-------------|
| `00_init.sql` | — | Active PostGIS, crée le schéma `ude` |
| `01_arrondissements.sql` | `ude.arrondissements` | Table de référence (20 arrondissements) — géométrie PostGIS, superficie_km2, population_2020, **densite_population** |
| `02_transactions_dvf.sql` | `ude.transactions_dvf` | Transactions DVF filtrées Paris, index géospatial |
| `03_logements_sociaux.sql` | `ude.logements_sociaux` | Logements sociaux financés (PLAI/PLUS/PLS) |
| `04_delinquance.sql` | `ude.delinquance` | Faits de délinquance par arrondissement × année × indicateur |
| `05_revenus_iris.sql` | `ude.revenus_iris` | Revenus FiLoSoFi 2018 à la maille IRIS |
| `06_indicateurs_gold.sql` | `ude.indicateurs_gold` | Table de faits Gold — agrégats prêts pour l'API (alimentée par Spark datamart.py) |
| `07_pipeline_runs.sql` | `ude.pipeline_runs` | Scaffolding monitoring pipelines (C2.4) — structure prête, alimentation Phase 2 |

## Notes importantes

- `ude.arrondissements.densite_population` est calculée par **Spark processor.py** (`population_totale / superficie_km2`) et stockée ici — l'API la lit directement sans recalcul.
- `ude.indicateurs_gold` est alimentée exclusivement par **Spark datamart.py** via JDBC.
- `ude.pipeline_runs` : structure DDL créée, alimentation depuis `@log_pipeline_run` de src\common\utils.py.

## Techno

PostgreSQL 15 + PostGIS 3.4 (image `postgis/postgis:15-3.4`)

## Exécution manuelle (si volume recréé)

```powershell
Get-Content sql\ddl\00_init.sql | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\01_arrondissements.sql | docker exec -i ude_postgres psql -U ude_user -d urban_data
# ... et ainsi de suite jusqu'à 07
```
