# =============================================================================
# run_pipeline.ps1  -  Pipeline complet Urban Data Explorer
# =============================================================================
# Flux :
#   Etape 1 : DDL PostgreSQL (schema idempotent)
#   Etape 2 : Feeders Python -> data/raw/
#               - feeder_logements_sociaux  (API opendata.paris.fr)
#               - feeder_airparif_batch     (API Airparif temps reel)
#               - feeder_revenus            (XLSX -> CSV, pandas, filtre Paris)
#   Etape 3 : Spark feeder    data/raw/ -> s3a://urban-data/bronze/
#   Etape 4 : Spark processor s3a://urban-data/bronze/ -> s3a://urban-data/silver/
#   Etape 5 : Spark datamart  s3a://urban-data/silver/ -> PostgreSQL
#
# Prerequis : docker compose up -d
# Usage     : .\scripts\run_pipeline.ps1
# =============================================================================

$ErrorActionPreference = "Stop"
$DATE = (Get-Date -Format "yyyy-MM-dd")

function Log($msg)    { Write-Host "`n>>> $msg" -ForegroundColor Cyan }
function OK($msg)     { Write-Host "    [OK] $msg" -ForegroundColor Green }
function Warn($msg)   { Write-Host "    [WARN] $msg" -ForegroundColor Yellow }
function Fail($msg)   { Write-Host "    [ERREUR] $msg" -ForegroundColor Red; exit 1 }
function Step($n, $t) { Write-Host "`n========== Etape $n : $t ==========" -ForegroundColor Magenta }

# =============================================================================
# PRE-FLIGHT
# =============================================================================
Log "Pre-flight - Verification des conteneurs UDE"

foreach ($c in @("ude_postgres", "ude_api", "ude_minio", "ude_spark_master")) {
    $s = docker inspect --format "{{.State.Status}}" $c 2>&1
    if ($LASTEXITCODE -ne 0) { Fail "$c introuvable. Lance : docker compose up -d" }
    if ($s -ne "running")    { Fail "$c n'est pas running (etat : $s)" }
}
OK "Stack UDE complete : postgres, api, minio, spark-master = running"

# =============================================================================
# ETAPE 1 : Schema PostgreSQL (DDL idempotent)
# =============================================================================
Step "1/5" "Schema PostgreSQL (DDL idempotent)"

$ddlFiles = @(
    "sql/ddl/00_init.sql",
    "sql/ddl/01_arrondissements.sql",
    "sql/ddl/02_transactions_dvf.sql",
    "sql/ddl/03_logements_sociaux.sql",
    "sql/ddl/04_delinquance.sql",
    "sql/ddl/05_revenus_iris.sql",
    "sql/ddl/06_indicateurs_gold.sql",
    "sql/ddl/07_pipeline_runs.sql"
)

foreach ($ddl in $ddlFiles) {
    if (-not (Test-Path $ddl)) { Warn "$ddl introuvable - ignore"; continue }
    Write-Host "    Applying $ddl ..." -ForegroundColor Gray
    Get-Content $ddl | docker exec -i ude_postgres psql -U ude_user -d urban_data -q
    if ($LASTEXITCODE -ne 0) { Fail "Echec DDL : $ddl" }
}
OK "Schema complet applique"

# =============================================================================
# ETAPE 2 : Feeders Python -> data/raw/
# =============================================================================
# Seuls les sources que Spark ne peut pas gerer directement :
#   - API REST  -> logements_sociaux, airparif (appels HTTP)
#   - XLSX      -> revenus (pandas convertit XLSX en CSV pour Spark)
# Les fichiers CSV/Parquet dans data/raw/ sont lus directement par Spark.
# =============================================================================
Step "2/5" "Feeders Python -> data/raw/"

Write-Host "    [2a] Logements sociaux (API opendata.paris.fr)..." -ForegroundColor Gray
docker exec ude_api python -m ingestion.api.feeder_logements_sociaux
if ($LASTEXITCODE -ne 0) { Fail "feeder_logements_sociaux" }
OK "logements_sociaux_raw.json -> data/raw/"

Write-Host "    [2b] Airparif qualite de l'air (API temps reel, 20 arrondissements)..." -ForegroundColor Gray
docker exec ude_api python -m ingestion.api.feeder_airparif_batch
if ($LASTEXITCODE -ne 0) {
    Warn "Airparif echoue (quota ou cle API) - pipeline continue sans donnees air"
} else {
    OK "airparif_$DATE.json -> data/raw/"
}

Write-Host "    [2c] Revenus INSEE FiLoSoFi (XLSX -> CSV Paris 870 IRIS)..." -ForegroundColor Gray
docker exec ude_api python -m ingestion.files.feeder_revenus
if ($LASTEXITCODE -ne 0) { Fail "feeder_revenus" }
OK "revenus_iris_paris.csv -> data/raw/"

# =============================================================================
# ETAPE 3 : Spark feeder  data/raw/ -> s3a://urban-data/bronze/
# =============================================================================
# Spark lit les fichiers bruts depuis /opt/spark-apps/data/raw/ (volume monte)
# et ecrit dans MinIO bronze en Parquet partitionne par date d'ingestion.
# Sources : DVF CSV, delinquance Parquet (filtre Paris), logements JSON,
#           revenus CSV, arrondissements CSV, airparif JSON.
# =============================================================================
Step "3/5" "Spark feeder (data/raw/ -> s3a://urban-data/bronze/)"

docker exec ude_spark_master bash /opt/spark-apps/submit/feeder.sh all $DATE
if ($LASTEXITCODE -ne 0) { Fail "feeder.sh echec (voir : docker compose logs spark-master)" }
OK "Toutes les sources dans MinIO s3a://urban-data/bronze/"

# =============================================================================
# ETAPE 4 : Spark processor  bronze/ -> silver/
# =============================================================================
# Nettoyage, normalisation, geocodage.
# Join arrondissements + revenus demographiques dans la couche silver.
# =============================================================================
Step "4/5" "Spark processor (s3a://urban-data/bronze/ -> s3a://urban-data/silver/)"

docker exec ude_spark_master bash /opt/spark-apps/submit/processor.sh all $DATE
if ($LASTEXITCODE -ne 0) { Fail "processor.sh echec (voir : docker compose logs spark-master)" }
OK "Donnees nettoyees dans MinIO s3a://urban-data/silver/"

# =============================================================================
# ETAPE 5 : Spark datamart  silver/ -> PostgreSQL
# =============================================================================
Step "5/5" "Spark datamart (s3a://urban-data/silver/ -> PostgreSQL)"

docker exec ude_spark_master bash /opt/spark-apps/submit/datamart.sh
if ($LASTEXITCODE -ne 0) { Fail "datamart.sh echec (voir : docker compose logs spark-master)" }
OK "Indicateurs gold charges dans ude.indicateurs_gold"

# =============================================================================
Write-Host "`n=============================================" -ForegroundColor Green
Write-Host "  PIPELINE TERMINE avec succes !" -ForegroundColor Green
Write-Host "  Date      : $DATE" -ForegroundColor Green
Write-Host "  Dashboard : http://localhost:8000/dashboard" -ForegroundColor Yellow
Write-Host "  API docs  : http://localhost:8000/docs" -ForegroundColor Yellow
Write-Host "  MinIO UI  : http://localhost:9001  (minioadmin / minioadmin)" -ForegroundColor Yellow
Write-Host "  Spark UI  : http://localhost:8080" -ForegroundColor Yellow
Write-Host "=============================================`n" -ForegroundColor Green

Start-Process "http://localhost:8000/dashboard"
