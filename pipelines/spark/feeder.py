"""
feeder.py -- Spark job : sources brutes -> MinIO /raw (partitionne par date d'ingestion)

Architecture : Raw Data -> feeder.py -> s3a://urban-data/raw/{source}/year=.../month=.../day=...

Parametres (spark-submit) :
  --source        dvf | delinquance | logements_sociaux | revenus | arrondissements | all
  --input-path    Repertoire local des fichiers sources (monte via volume Docker)
  --output-path   Chemin de sortie MinIO S3A  (ex: s3a://urban-data/raw)
  --date          Date d'ingestion YYYY-MM-DD (defaut: aujourd'hui)

Usage (depuis le conteneur ude_spark_master) :
  docker exec ude_spark_master bash /opt/spark-apps/submit/feeder.sh dvf 2026-06-24
  docker exec ude_spark_master bash /opt/spark-apps/submit/feeder.sh all 2026-06-24

Notes :
  - Spark (bitnami/spark:3.5) integre dans la stack UDE (docker-compose.yml)
  - Donnees montees via volume : ./data -> /opt/spark-apps/data  (pas de docker cp)
  - MinIO accessible depuis les conteneurs Spark sur http://minio:9000
  - PostgreSQL accessible via hostname "postgres" port 5432 (meme compose network)
  - Aucune dependance pandas -- tout est lu en natif Spark (CSV/Parquet/JSON/spark-excel)
"""

import argparse
import os
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# ─── Session Spark ────────────────────────────────────────────────────────────

def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


# ─── Parseur d'arguments ─────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="UDE Feeder — sources brutes → HDFS /raw")
    p.add_argument(
        "--source", required=True,
        choices=["dvf", "delinquance", "logements_sociaux", "revenus", "arrondissements", "all"],
        help="Source à ingérer"
    )
    p.add_argument(
        "--input-path", required=True,
        help="Répertoire local des fichiers sources (ex: /opt/spark-jobs/data/raw)"
    )
    p.add_argument(
        "--output-path", required=True,
        help="Chemin de sortie HDFS (ex: hdfs://namenode:9000/urban-data/raw)"
    )
    p.add_argument(
        "--date", default=date.today().isoformat(),
        help="Date d'ingestion YYYY-MM-DD (défaut: aujourd'hui)"
    )
    return p.parse_args()


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _partition_cols(y: str, m: str, d: str):
    """Ajoute les colonnes de partition date."""
    return [
        lit(y).alias("ingestion_year"),
        lit(m).alias("ingestion_month"),
        lit(d).alias("ingestion_day"),
    ]


def _write_partitioned(df, output_path: str, source: str):
    (
        df.write
        .mode("overwrite")
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
        .parquet(f"{output_path}/{source}/")
    )
    print(f"[feeder/{source}] ✓ Écrit dans {output_path}/{source}/")


# ─── Ingestion DVF (CSV multi-fichiers) ──────────────────────────────────────

def ingest_dvf(spark: SparkSession, input_path: str, output_path: str,
               y: str, m: str, d: str) -> None:
    """
    Lit tous les CSV DVF du répertoire input_path.
    cache() : le DataFrame est réutilisé pour count() de validation puis écriture.
    Visible dans Spark UI sous l'onglet Storage.
    """
    dvf_pattern = os.path.join(input_path, "dvf_75_*.csv")
    print(f"[feeder/dvf] Lecture de {dvf_pattern}")

    df = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("inferSchema", "false")   # tout en string — nettoyage dans processor
        .csv(dvf_pattern)
        .select("*", *_partition_cols(y, m, d))
    )

    # cache() : sera lu deux fois (count + write) — visible dans Spark UI Storage
    df = df.cache()

    nb = df.count()
    print(f"[feeder/dvf] {nb:,} lignes lues ({nb // 6:,} transactions/an en moyenne)")

    _write_partitioned(df, output_path, "dvf")
    df.unpersist()


# ─── Ingestion Délinquance (Parquet) ─────────────────────────────────────────

def ingest_delinquance(spark: SparkSession, input_path: str, output_path: str,
                       y: str, m: str, d: str) -> None:
    """
    Lit data/raw/delinquance.parquet (France entiere) et filtre sur Paris en Spark.
    Codes INSEE Paris : 75101 a 75120 (colonne CODGEO_2025).
    """
    from pyspark.sql.functions import col as spark_col

    src = os.path.join(input_path, "delinquance.parquet")
    if not os.path.exists(src):
        raise FileNotFoundError(f"Fichier delinquance introuvable : {src}")

    print(f"[feeder/delinquance] Lecture Raw (France entiere) : {src}")

    paris_codes = [str(75100 + i) for i in range(1, 21)]  # 75101 a 75120

    df = (
        spark.read.parquet(src)
        .filter(spark_col("CODGEO_2025").isin(paris_codes))
        .select("*", *_partition_cols(y, m, d))
        .cache()
    )

    nb = df.count()
    print(f"[feeder/delinquance] {nb:,} enregistrements Paris (filtres depuis France entiere)")

    _write_partitioned(df, output_path, "delinquance")
    df.unpersist()

def ingest_logements_sociaux(spark: SparkSession, input_path: str, output_path: str,
                              y: str, m: str, d: str) -> None:
    """
    Lit le fichier logements_sociaux_raw.json (bronze, déjà récupéré via API).
    Chemin input_path peut pointer vers data/bronze/ ou un bucket staging.
    cache() : count() de validation + écriture.
    """
    # Supporte chemin local (bronze) ou s3a staging
    src = os.path.join(input_path, "logements_sociaux_raw.json")
    if not src.startswith("hdfs://") and not os.path.exists(src):
        # Fallback vers data/bronze/
        bronze = os.path.join(
            os.path.dirname(input_path), "bronze", "logements_sociaux_raw.json"
        )
        if os.path.exists(bronze):
            src = bronze
            print(f"[feeder/logements_sociaux] Fallback vers {src}")

    print(f"[feeder/logements_sociaux] Lecture de {src}")

    df = (
        spark.read
        .option("multiline", "true")
        .json(src)
        .select("*", *_partition_cols(y, m, d))
        .cache()
    )

    nb = df.count()
    print(f"[feeder/logements_sociaux] {nb:,} enregistrements")

    _write_partitioned(df, output_path, "logements_sociaux")
    df.unpersist()


# ─── Ingestion Revenus INSEE FiLoSoFi (CSV, séparateur ;) ────────────────────

def ingest_revenus(spark: SparkSession, input_path: str, output_path: str,
                   y: str, m: str, d: str) -> None:
    """
    Lit data/raw/revenus_iris_paris.csv (CSV Paris, prepare par feeder_revenus.py).
    feeder_revenus.py convertit le XLSX FiLoSoFi -> CSV filtre Paris (870 IRIS).
    Spark ne lit pas XLSX nativement (spark-excel incompatible Scala 2.13).
    """
    src = os.path.join(input_path, "revenus_iris_paris.csv")
    if not os.path.exists(src):
        raise FileNotFoundError(
            f"CSV revenus introuvable : {src}\n"
            f"Lance d'abord : docker exec ude_api python -m ingestion.files.feeder_revenus"
        )

    print(f"[feeder/revenus] Lecture CSV raw : {src}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(src)
        .select("*", *_partition_cols(y, m, d))
        .cache()
    )
    nb = df.count()
    print(f"[feeder/revenus] {nb:,} IRIS Paris")

    _write_partitioned(df, output_path, "revenus")
    df.unpersist()


# ─── Ingestion Arrondissements (CSV superficie + population hardcodée) ────────

def ingest_arrondissements(spark: SparkSession, input_path: str, output_path: str,
                            y: str, m: str, d: str) -> None:
    """
    Lit arrondissements.csv (séparateur ;, colonne Surface en m²) avec Spark natif.
    Population 2020 hardcodée — données de référence INSEE fixes (20 lignes).
    Aucune dépendance pandas ni XLSX dans le conteneur Spark.
    cache() : count() de validation + écriture.
    """
    from pyspark.sql import Row
    from pyspark.sql.functions import col as spark_col, round as spark_round

    # Population 2020 — source INSEE recensement (données de référence fixes)
    population_2020 = {
        1: 16266,  2: 21559,  3: 34576,  4: 28088,  5: 58850,
        6: 41100,  7: 51765,  8: 36808,  9: 59895,  10: 90372,
        11: 147476, 12: 142327, 13: 181556, 14: 135964, 15: 233484,
        16: 165820, 17: 167476, 18: 195233, 19: 184389, 20: 196004,
    }

    csv_path = os.path.join(input_path, "arrondissements.csv")
    print(f"[feeder/arrondissements] Lecture CSV : {csv_path}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .csv(csv_path)
    )

    # Colonne numéro arrondissement : contient "Num", pas "INSEE"
    num_col = [c for c in df_raw.columns if "Num" in c and "INSEE" not in c][0]

    df_sup = (
        df_raw
        .select(
            spark_col(num_col).cast("int").alias("arrondissement"),
            spark_col("Surface").cast("double").alias("surface_m2"),
        )
        .filter(spark_col("arrondissement").between(1, 20))
        .withColumn("superficie_km2",
                    spark_round(spark_col("surface_m2") / 1_000_000, 3))
    )

    # Population : DataFrame depuis dict Python (20 lignes, driver side)
    df_pop = spark.createDataFrame(
        [Row(arrondissement=k, population_totale=v)
         for k, v in population_2020.items()]
    )

    df = (
        df_sup.join(df_pop, "arrondissement")
        .select("arrondissement", "superficie_km2", "population_totale")
        .select("*", *_partition_cols(y, m, d))
        .cache()
    )

    nb = df
    nb = df.count()
    print(f"[feeder/arrondissements] {nb} arrondissements")

    _write_partitioned(df, output_path, "arrondissements")
    df.unpersist()


# --- Ingestion Airparif (JSON temps reel) ------------------------------------

def ingest_airparif(spark: SparkSession, input_path: str, output_path: str,
                    y: str, m: str, d: str) -> None:
    """
    Lit le fichier JSON Airparif du jour depuis data/raw/.
    Fichier produit par feeder_airparif_batch.py (appel API 20 arrondissements).
    """
    src = os.path.join(input_path, f"airparif_{y}-{m}-{d}.json")
    if not os.path.exists(src):
        print(f"[feeder/airparif] Fichier absent (skip) : {src}")
        return

    print(f"[feeder/airparif] Lecture JSON : {src}")
    df = (
        spark.read
        .option("multiline", "true")
        .json(src)
        .select("*", *_partition_cols(y, m, d))
        .cache()
    )
    nb = df.count()
    print(f"[feeder/airparif] {nb:,} enregistrements qualite air")
    _write_partitioned(df, output_path, "airparif")
    df.unpersist()


# --- Dispatch ----------------------------------------------------------------

FEEDERS = {
    "dvf":               ingest_dvf,
    "delinquance":       ingest_delinquance,
    "logements_sociaux": ingest_logements_sociaux,
    "revenus":           ingest_revenus,
    "arrondissements":   ingest_arrondissements,
    "airparif":          ingest_airparif,
}

ALL_SOURCES = list(FEEDERS.keys())


# --- Main --------------------------------------------------------------------

def main():
    args = parse_args()
    y, m, d = args.date.split("-")

    spark = get_spark(f"UDE-Feeder-{args.source}-{args.date}")

    sources = ALL_SOURCES if args.source == "all" else [args.source]

    print(f"\n[feeder] Sources a ingerer : {sources}")
    print(f"[feeder] Input  : {args.input_path}")
    print(f"[feeder] Output : {args.output_path}")
    print(f"[feeder] Date   : {args.date}\n")

    errors = []
    for source in sources:
        try:
            print(f"\n{'='*60}")
            print(f"  Ingestion : {source}")
            print(f"{'='*60}")
            FEEDERS[source](spark, args.input_path, args.output_path, y, m, d)
        except Exception as e:
            print(f"[feeder/{source}] ERREUR : {e}")
            errors.append((source, str(e)))

    spark.stop()

    if errors:
        print(f"\n[feeder] {len(errors)} source(s) en erreur :")
        for src, err in errors:
            print(f"  - {src}: {err}")
        raise SystemExit(1)

    print(f"\n[feeder] Toutes les sources ingeries avec succes.")


if __name__ == "__main__":
    main()
