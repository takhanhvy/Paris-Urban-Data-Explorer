# Commandes du projet — PowerShell (Windows)

Toutes les commandes à lancer depuis la racine du projet dans PowerShell.

---

## Stack Docker

```powershell
# Premier démarrage (build l'image API custom + lance tous les services)
docker compose up -d --build

# Relancer sans rebuild
docker compose up -d

# Vérifier l'état des conteneurs
docker compose ps

# Logs d'un service
docker compose logs -f ude_api
docker compose logs -f ude_spark_master

# Arrêter (sans supprimer les volumes)
docker compose down

# Reset complet (supprime tous les volumes — données perdues)
docker compose down -v
docker compose up -d --build
```

---

## Pipeline complet (script automatisé)

```powershell
.\scripts\run_pipeline.ps1
```

Le script enchaîne : DDL PostgreSQL → feeders Python → Spark feeder → Spark processor → Spark datamart.

---

## Étapes manuelles détaillées

### Étape 1 — Feeders Python (API → data/raw/)

Tous les feeders écrivent dans `data/raw/` — la **landing zone commune** à toutes les sources.

```powershell
# Logements sociaux → data/raw/logements_sociaux_raw.json
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux

# Airparif batch → data/raw/airparif_YYYY-MM-DD.json
# (≠ streaming Kafka — voir section "Streaming Airparif" pour le KPI live)
docker exec ude_api python -m ingestion.api.feeder_airparif_batch

# Revenus FiLoSoFi XLSX → data/raw/revenus_iris_paris.csv
docker exec ude_api python -m ingestion.files.feeder_revenus
```

### Étape 2 — Spark feeder : data/raw/ → MinIO bronze

```powershell
# Toutes les sources (dvf, delinquance, logements_sociaux, revenus, arrondissements)
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh all

# Source spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh dvf 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh logements_sociaux 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh delinquance 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh revenus 2026-06-24
```

### Étape 3 — Spark processor : MinIO bronze → MinIO silver

```powershell
# Toutes les sources
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh all

# Source spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh dvf 2026-06-24
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh delinquance 2026-06-24
```

### Étape 4 — Spark datamart : MinIO silver → PostgreSQL

```powershell
# Toutes les années (2020–2025)
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh

# Année spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024

# Année spécifique en mode append
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024 append
```

---

## DDL PostgreSQL (si volume recréé)

Les scripts s'appliquent automatiquement au premier démarrage. Pour forcer manuellement :

```powershell
foreach ($f in "00_init","01_arrondissements","02_transactions_dvf","03_logements_sociaux","04_delinquance","05_revenus_iris","06_indicateurs_gold","07_pipeline_runs") {
    Get-Content "sql\ddl\$f.sql" | docker exec -i ude_postgres psql -U ude_user -d urban_data -q
}
```

---

## Streaming Airparif via Kafka

> **C'est ce chemin qui alimente le KPI "Qualité de l'air" du dashboard.** Le pipeline Spark batch (`feeder_airparif_batch.py`) écrit dans `data/raw/` comme les autres sources, mais le dashboard lit MongoDB. Pour que le KPI s'affiche, les deux processus ci-dessous doivent tourner.

```powershell
# Terminal 1 — Producer (API Airparif → topic Kafka airparif.quality, toutes les heures)
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer (topic Kafka → MongoDB air_quality, TTL 24h)
python -m ingestion.streaming.airparif_consumer
```

Si MongoDB `air_quality` est vide, le KPI affiche `N/A` (timeout 1 s, échec silencieux).

---

## Vérifications et debug

```powershell
# Test API — health check
Invoke-RestMethod "http://localhost:8000/health"

# Test API — métriques d'un arrondissement
Invoke-RestMethod "http://localhost:8000/api/metrics?year=2024&arrondissement=75107"

# PostgreSQL — nombre de lignes gold
docker exec ude_postgres psql -U ude_user -d urban_data -c "SELECT COUNT(*) FROM ude.indicateurs_gold;"

# PostgreSQL — résumé par année
docker exec ude_postgres psql -U ude_user -d urban_data -c "SELECT annee, COUNT(*) FROM ude.indicateurs_gold GROUP BY annee ORDER BY annee;"

# PostgreSQL — shell interactif
docker exec -it ude_postgres psql -U ude_user -d urban_data

# MongoDB — nombre de docs Airparif
docker exec -i $(docker compose ps -q mongodb) mongosh urban_data_nosql --eval "db.air_quality.countDocuments()"

# MinIO — lister les buckets et le contenu bronze
# → Interface web : http://localhost:9001 (minioadmin / minioadmin)
```

---

## Interfaces web

| Interface | URL |
|-----------|-----|
| Dashboard | http://localhost:8000/dashboard/ |
| API Swagger | http://localhost:8000/docs |
| Spark UI | http://localhost:8080 |
| MinIO console | http://localhost:9001 |

---

## Relancer un service

```powershell
# Après modification du code FastAPI
docker compose restart api

# Rebuild complet de l'image API
docker compose build api && docker compose up -d api
```

## Nettoyage local

```powershell
# Supprimer les caches Python
Get-ChildItem -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -Filter "*.pyc" | Remove-Item -Force
```
