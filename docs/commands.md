# Commandes du projet — PowerShell (Windows)

Toutes les commandes à lancer depuis la racine du projet dans PowerShell.

---

## Stack Docker

```powershell
# Lancer tous les services (PostgreSQL, MongoDB, Kafka, API)
docker-compose up -d

# Arrêter tous les services
docker-compose down

# Voir les logs d'un service
docker-compose logs -f ude_api
docker-compose logs -f ude_postgres

# Vérifier l'état des conteneurs
docker-compose ps
```

---

## Initialisation de la base PostgreSQL

À exécuter une seule fois, dans l'ordre :

```powershell
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/00_init.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/01_arrondissements.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/02_transactions_dvf.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/03_logements_sociaux.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/04_delinquance.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/05_revenus_iris.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/06_indicateurs_gold.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data -f /docker-entrypoint-initdb.d/07_pipeline_runs.sql
```

---

## Réinitialisation PostgreSQL (si les tables n'existent pas)

Si le volume a été recréé et que les scripts DDL n'ont pas tourné automatiquement, utiliser la redirection stdin depuis l'hôte :

```powershell
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/00_init.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/01_arrondissements.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/02_transactions_dvf.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/03_logements_sociaux.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/04_delinquance.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/05_revenus_iris.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/06_indicateurs_gold.sql
docker exec -i ude_postgres psql -U ude_user -d urban_data < sql/ddl/07_pipeline_runs.sql
```

Pour repartir de zéro (supprime toutes les données PostgreSQL) :

```powershell
docker compose down
docker volume rm paris-urban-data-explorer_postgres_data
docker compose up -d postgres
# Attendre ~20 sec, puis relancer les DDL ci-dessus si nécessaire
```

---

## Ingestion batch

```powershell
# DVF (CSV → Bronze Parquet)
python -m ingestion.files.feeder_dvf

# Logements sociaux (API → Bronze JSON)
python -m ingestion.api.feeder_logements_sociaux

# Délinquance (Parquet → Bronze Parquet filtré)
python -m ingestion.files.feeder_delinquance

# Revenus (XLSX → Bronze Parquet)
python -m ingestion.files.feeder_revenus

# Tout en séquence
python -m ingestion.files.feeder_dvf
python -m ingestion.api.feeder_logements_sociaux
python -m ingestion.files.feeder_delinquance
python -m ingestion.files.feeder_revenus
```

---

## Pipelines Silver

```powershell
python -m pipelines.silver.processor_dvf
python -m pipelines.silver.processor_logements_sociaux
python -m pipelines.silver.processor_delinquance
python -m pipelines.silver.processor_revenus
```

---

## Pipeline Gold

```powershell
python -m pipelines.gold.processor_gold
```

---

## Streaming Airparif (2 terminaux séparés)

```powershell
# Terminal 1 — Producer (envoie dans Kafka)
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer (Kafka → MongoDB)
python -m ingestion.streaming.airparif_consumer
```

---

## API FastAPI

```powershell
# Mode développement (hot reload)
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Docs disponibles sur : http://localhost:8000/docs
```

---

## Tests

```powershell
pytest tests/ -v
```

---

## Nettoyage

```powershell
# Supprimer les caches Python
Get-ChildItem -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -Filter "*.pyc" | Remove-Item -Force
```
