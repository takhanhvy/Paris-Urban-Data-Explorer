#!/bin/bash
# ============================================================
# submit/datamart.sh -- Lance datamart.py
#
# Lit depuis  : s3a://urban-data/silver/ (MinIO)
# Ecrit dans  : PostgreSQL ude.indicateurs_gold
#
# Usage :
#   bash /opt/spark-apps/submit/datamart.sh [annee]
#   bash /opt/spark-apps/submit/datamart.sh 2024
#   bash /opt/spark-apps/submit/datamart.sh       # toutes les annees
# ============================================================

set -euo pipefail

ANNEE="${1:-}"
WRITE_MODE="${2:-overwrite}"

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
SILVER_PATH="s3a://urban-data/silver"

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-urban_data}"
POSTGRES_USER="${POSTGRES_USER:-ude_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-changeme}"

JDBC_URL="jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ANNEE_ARG=""
if [ -n "${ANNEE}" ]; then
  ANNEE_ARG="--annee ${ANNEE}"
fi

echo "========================================"
echo "  UDE Datamart : MinIO silver -> PostgreSQL"
echo "  annee  : ${ANNEE:-toutes}"
echo "  silver : ${SILVER_PATH}"
echo "  jdbc   : ${JDBC_URL}"
echo "  mode   : ${WRITE_MODE}"
echo "========================================"

/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --name "UDE-Datamart-${ANNEE:-all}" \
  --packages "org.postgresql:postgresql:42.7.3" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.sql.shuffle.partitions=8" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=us-east-1" \
  "${SCRIPT_DIR}/datamart.py" \
  --silver-path "${SILVER_PATH}" \
  --jdbc-url "${JDBC_URL}" \
  --jdbc-user "${POSTGRES_USER}" \
  --jdbc-password "${POSTGRES_PASSWORD}" \
  --write-mode "${WRITE_MODE}" \
  ${ANNEE_ARG}
