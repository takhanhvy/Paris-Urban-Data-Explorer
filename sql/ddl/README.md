# sql/ddl — Schéma relationnel PostgreSQL (C1.1)

Scripts DDL à exécuter dans l'ordre (numérotés) au premier démarrage.

| Fichier | Table | Description |
|---------|-------|-------------|
| `00_init.sql` | — | Active PostGIS, crée le schéma `ude` |
| `01_arrondissements.sql` | `ude.arrondissements` | Table de référence (20 arrondissements) avec géométrie PostGIS |
| `02_transactions_dvf.sql` | `ude.transactions_dvf` | Transactions DVF filtrées Paris, avec index géospatial |
| `03_logements_sociaux.sql` | `ude.logements_sociaux` | Logements sociaux financés, tous types (PLAI/PLUS/PLS) |
| `04_delinquance.sql` | `ude.delinquance` | Faits de délinquance par arrondissement × année × indicateur |
| `05_revenus_iris.sql` | `ude.revenus_iris` | Revenus FiLoSoFi 2018 à la maille IRIS |
| `06_indicateurs_gold.sql` | `ude.indicateurs_gold` | Table de faits Gold — agrégats prêts pour l'API |
| `07_pipeline_runs.sql` | `ude.pipeline_runs` | Monitoring des pipelines (C2.4) |

**Techno :** PostgreSQL 15 + PostGIS 3.4

**Dépendances :** image Docker `postgis/postgis:15-3.4`

**Exécution :** `make db-init` ou dans Docker : `docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/00_init.sql`
