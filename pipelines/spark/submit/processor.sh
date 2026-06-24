#!/bin/bash
# ============================================================
# submit/processor.sh -- Lance processor.py
#
# Lit depuis  : s3a://urban-data/bronze/ (MinIO)
# Ecrit dans  : s3a://urban-data/silver/ (MinIO)
# Join silver : arrondissements + donnees demographiques
#
# Usage :
#   bash /opt/spark-apps/submit/processor.sh [source] [date]
#   bash /opt/spark-apps/submit/processor.sh all 2026-06-24
# ============================================================

set -euo pipefail

SOURCE="${1:-all}"
DATE="${2:-$(date +%Y-%m-%d)}"

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
BRONZE_PATH="s3a://urban-data/bronze"
SILVER_PATH="s3a://urban-data/silver"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "========================================"
echo "  UDE Processor : MinIO bronze -> MinIO silver"
echo "  source : ${SOURCE}"
echo "  date   : ${DATE}"
echo "  bronze : ${BRONZE_PATH}"
echo "  silver : ${SILVER_PATH}"
echo "========================================"

# hadoop-aws pre-installe dans l'image (Dockerfile.spark) : pas de --packages S3A
/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --name "UDE-Processor-${SOURCE}-${DATE}" \
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
  "${SCRIPT_DIR}/processor.py" \
  --source "${SOURCE}" \
  --bronze-path "${BRONZE_PATH}" \
  --silver-path "${SILVER_PATH}" \
  --date "${DATE}"
