# Commandes du projet — PowerShell (Windows)

Toutes les commandes à lancer depuis la racine du projet dans PowerShell.

---

## Stack Docker

```powershell
# Premier démarrage (build les images Spark custom + lance tout)
docker compose up -d --build

# Relancer sans rebuild
docker compose up -d

# Vérifier l'état des conteneurs
docker compose ps

# Logs d'un service
docker compose logs -f ude_api
docker compose logs -f ude_spark_master

# Arrêter
docker compose down
```

---

## Initialisation PostgreSQL

Les scripts DDL s'exécutent automatiquement au premier démarrage via `docker-entrypoint-initdb.d/`.
Si besoin de forcer manuellement (volume recréé) :

```powershell
Get-Content sql\ddl\00_init.sql              | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\01_arrondissements.sql   | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\02_transactions_dvf.sql  | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\03_logements_sociaux.sql | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\04_delinquance.sql       | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\05_revenus_iris.sql      | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\06_indicateurs_gold.sql  | docker exec -i ude_postgres psql -U ude_user -d urban_data
Get-Content sql\ddl\07_pipeline_runs.sql     | docker exec -i ude_postgres psql -U ude_user -d urban_data
```

Repartir de zéro :

```powershell
docker compose down -v
docker compose up -d --build
```

---

## Pipeline Spark complet

Les jobs sont soumis au cluster Spark Standalone via les scripts dans `pipelines/spark/submit/`.
Les données transitent par HDFS : `hdfs://namenode:9000/urban-data/`.

### Étape 0 — Logements sociaux (API → bronze local)

```powershell
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux
```

### Étape 1 — Feeder : data/raw + data/bronze → HDFS /raw

```powershell
# Toutes les sources (dvf, delinquance, logements_sociaux, revenus, arrondissements)
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh all

# Source spécifique + date
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh dvf 2026-06-23
docker exec ude_spark_master bash /opt/spark-jobs/submit/feeder.sh logements_sociaux 2026-06-23
```

### Étape 2 — Processor : HDFS /raw → HDFS /silver

```powershell
# Toutes les sources
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh all

# Source spécifique + date
docker exec ude_spark_master bash /opt/spark-jobs/submit/processor.sh dvf 2026-06-23
```

### Étape 3 — Datamart : HDFS /silver → PostgreSQL

```powershell
# Toutes les années
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh

# Année spécifique
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024

# Avec write mode append
docker exec ude_spark_master bash /opt/spark-jobs/submit/datamart.sh 2024 append
```

---

## Ingestion Airparif

### Batch (injecte directement dans MongoDB)

```powershell
docker exec ude_api python ingestion/api/feeder_airparif_batch.py
```

### Données de test Airparif (si l'API n'est pas disponible)

```powershell
docker exec -i $(docker compose ps -q mongodb) mongosh urban_data_nosql --eval "
db.air_quality.deleteMany({});
var labels = ['Très bon','Bon','Bon','Moyen','Dégradé','Bon','Bon','Très bon','Bon','Moyen',
              'Bon','Bon','Dégradé','Bon','Bon','Très bon','Bon','Moyen','Bon','Bon'];
for (var i = 1; i <= 20; i++) {
  var insee = i < 10 ? '7510' + i : '751' + i;
  db.air_quality.replaceOne(
    {insee: insee},
    {insee: insee, data: [{date: '2026-06-23', label: labels[i-1], code_qual: 2}],
     inserted_at: new Date(), timestamp: new Date().toISOString()},
    {upsert: true}
  );
}
print('Docs insérés : ' + db.air_quality.countDocuments());
"
```

### Streaming Airparif via Kafka (2 terminaux)

```powershell
# Terminal 1 — Producer (appelle API Airparif → Kafka)
python -m ingestion.streaming.airparif_producer

# Terminal 2 — Consumer (Kafka → MongoDB)
python -m ingestion.streaming.airparif_consumer
```

---

## Vérifications

```powershell
# Test API — métriques d'un arrondissement
Invoke-RestMethod "http://localhost:8000/api/metrics?year=2024&arrondissement=75107"

# Vérifier PostgreSQL — nombre de lignes gold
docker exec ude_postgres psql -U ude_user -d urban_data -c "SELECT COUNT(*) FROM ude.indicateurs_gold;"

# Vérifier MongoDB — documents Airparif
docker exec -i $(docker compose ps -q mongodb) mongosh urban_data_nosql --eval "db.air_quality.countDocuments()"

# Vérifier HDFS — lister les répertoires
docker exec ude_namenode hdfs dfs -ls /urban-data/
docker exec ude_namenode hdfs dfs -ls /urban-data/raw/
docker exec ude_namenode hdfs dfs -ls /urban-data/silver/
```

---

## Interfaces web

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Dashboard | http://localhost:8000/dashboard | — |
| API Swagger | http://localhost:8000/docs | — |
| Spark Master UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |

---

## Redémarrage API

Après modification du code FastAPI :

```powershell
docker compose restart api
```

---

## Nettoyage

```powershell
# Supprimer les caches Python (local)
Get-ChildItem -Recurse -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -Filter "*.pyc" | Remove-Item -Force

# Arrêter et supprimer tous les volumes (reset complet)
docker compose down -v

# Rebuild les images Spark (si Dockerfile.spark-master/worker modifié)
docker compose build spark-master spark-worker
```
