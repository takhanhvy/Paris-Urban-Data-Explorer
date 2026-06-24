#!/bin/bash
# ============================================================
# submit/feeder.sh -- Lance feeder.py
#
# Lit depuis  : /opt/spark-apps/data/bronze/ (volume local)
# Ecrit dans  : s3a://urban-data/bronze/     (MinIO)
#
# Usage :
#   bash /opt/spark-apps/submit/feeder.sh [source] [date]
#   bash /opt/spark-apps/submit/feeder.sh all 2026-06-24
# ============================================================

set -euo pipefail

SOURCE="${1:-all}"
DATE="${2:-$(date +%Y-%m-%d)}"

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
INPUT_PATH="/opt/spark-apps/data/raw"
OUTPUT_PATH="s3a://urban-data/bronze"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "========================================"
echo "  UDE Feeder : local bronze -> MinIO bronze"
echo "  source  : ${SOURCE}"
echo "  date    : ${DATE}"
echo "  input   : ${INPUT_PATH}"
echo "  output  : ${OUTPUT_PATH}"
echo "========================================"

# hadoop-aws est pre-installe dans l'image (Dockerfile.spark) : pas de --packages S3A
# Seul le driver PostgreSQL est telecharge a chaud si besoin (ne cree pas de conflit Hadoop)
/opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER}" \
  --name "UDE-Feeder-${SOURCE}-${DATE}" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=2g" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.endpoint.region=us-east-1" \
  "${SCRIPT_DIR}/feeder.py" \
  --source "${SOURCE}" \
  --input-path "${INPUT_PATH}" \
  --output-path "${OUTPUT_PATH}" \
  --date "${DATE}"
