"""
processor.py — Spark job : HDFS /raw → HDFS /silver (nettoyage + normalisation)

Architecture : hdfs://namenode:9000/urban-data/raw/{source}/ → .../silver/{source}/

Paramètres :
  --source        dvf | delinquance | logements_sociaux | revenus | arrondissements | all
  --bronze-path      Chemin HDFS /raw    (ex: hdfs://namenode:9000/urban-data/raw)
  --silver-path   Chemin HDFS /silver (ex: hdfs://namenode:9000/urban-data/silver)
  --date          Partition à traiter YYYY-MM-DD (défaut: aujourd'hui)

Optimisation Spark :
  persist(MEMORY_AND_DISK) après lecture + filtre initial — le DataFrame nettoyé
  est réutilisé pour : count() de validation, stats qualité, et écriture silver.
  Visible dans Spark UI > Storage.

Usage (depuis le conteneur spark-master du cluster Marcel-Jan) :
  docker exec spark-master /opt/spark-apps/submit/processor.sh dvf 2026-06-24

  Ou directement :
  spark-submit --master spark://spark-master:7077 \\
    --name "UDE-Processor-dvf" \\
    --conf "spark.driver.memory=1g" \\
    --conf "spark.executor.memory=2g" \\
    /opt/spark-apps/processor.py \\
    --source dvf \\
    --bronze-path hdfs://namenode:9000/urban-data/raw \\
    --silver-path hdfs://namenode:9000/urban-data/silver \\
    --date 2026-06-24
"""

import argparse
from datetime import date, datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ─── Session Spark ────────────────────────────────────────────────────────────

def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ─── Parseur d'arguments ─────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="UDE Processor — HDFS /raw → /silver")
    p.add_argument(
        "--source", required=True,
        choices=["dvf", "delinquance", "logements_sociaux", "revenus", "arrondissements", "residences_principales", "all"],
    )
    p.add_argument("--bronze-path", required=True,
                   help="Chemin HDFS /raw  (ex: hdfs://namenode:9000/urban-data/raw)")
    p.add_argument("--silver-path", required=True,
                   help="Chemin HDFS /silver (ex: hdfs://namenode:9000/urban-data/silver)")
    p.add_argument("--date", default=date.today().isoformat(),
                   help="Partition à traiter YYYY-MM-DD")
    return p.parse_args()


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _read_raw_partition(spark: SparkSession, bronze_path: str, source: str,
                        y: str, m: str, d: str) -> DataFrame:
    """Lit la partition date d'un répertoire raw HDFS."""
    path = f"{bronze_path}/{source}/ingestion_year={y}/ingestion_month={m}/ingestion_day={d}"
    return spark.read.parquet(path)


def _write_silver(df: DataFrame, silver_path: str, source: str,
                  y: str, m: str, d: str) -> None:
    (
        df
        .withColumn("ingestion_year", F.lit(y))
        .withColumn("ingestion_month", F.lit(m))
        .withColumn("ingestion_day", F.lit(d))
        .write
        .mode("overwrite")
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
        .parquet(f"{silver_path}/{source}/")
    )
    print(f"[processor/{source}] ✓ Silver écrit dans {silver_path}/{source}/")


# ─── Processor DVF ───────────────────────────────────────────────────────────

def process_dvf(spark: SparkSession, bronze_path: str, silver_path: str,
                y: str, m: str, d: str) -> None:
    """
    Nettoyage DVF :
    - Filtre Paris (code_commune IN 75101..75120)
    - Cast des types (valeur_fonciere, surface, date_mutation)
    - Calcul prix_m2 = valeur_fonciere / surface_reelle_bati
    - Extraction arrondissement (code_commune → int 1..20)
    - Filtre : type_local = 'Appartement', surface > 9

    persist(MEMORY_AND_DISK) : réutilisé pour count(), stats qualité, écriture.
    """
    df_raw = _read_raw_partition(spark, bronze_path, "dvf", y, m, d)
    print(f"[processor/dvf] {df_raw.count():,} lignes brutes lues")

    # Filtre Paris + cast types + calcul prix_m2
    # NOTE: F.col().rlike() et F.col().between() sont incompatibles avec le
    # HybridAnalyzer de Spark 4.x → utiliser SQL string expressions dans filter()
    df_clean = (
        df_raw
        .filter("code_commune rlike '^7510[0-9]$|^75110$|^7511[0-9]$|^75120$'")
        .filter("type_local = 'Appartement'")
        .withColumn("valeur_fonciere",
                    F.regexp_replace("valeur_fonciere", ",", ".").cast("double"))
        .withColumn("surface_reelle_bati",
                    F.regexp_replace("surface_reelle_bati", ",", ".").cast("double"))
        .withColumn("nombre_pieces_principales",
                    F.col("nombre_pieces_principales").cast("int"))
        .withColumn("date_mutation",
                    F.to_date("date_mutation", "yyyy-MM-dd"))
        .filter("surface_reelle_bati > 9")
        .filter("valeur_fonciere > 10000")
        .withColumn("prix_m2",
                    F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 2))
        .withColumn("annee", F.year("date_mutation"))
        .withColumn("arrondissement",
                    (F.col("code_commune").cast("int") - 75100).cast("smallint"))
        # Filtre prix aberrants (< 500 ou > 80 000 €/m²)
        .filter("prix_m2 between 500 and 80000")
        .select(
            "date_mutation", "annee", "arrondissement", "code_commune",
            "valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales",
            "type_local", "prix_m2", "nature_mutation",
        )
    )

    # persist(MEMORY_AND_DISK) : réutilisé 3× ci-dessous — visible Spark UI Storage
    df_clean = df_clean.persist(StorageLevel.MEMORY_AND_DISK)

    nb = df_clean.count()
    print(f"[processor/dvf] {nb:,} transactions Paris après nettoyage")

    # Statistiques qualité (utilise le cache)
    stats = df_clean.agg(
        F.min("prix_m2").alias("prix_min"),
        F.max("prix_m2").alias("prix_max"),
        F.avg("prix_m2").alias("prix_moy"),
        F.countDistinct("annee").alias("nb_annees"),
    ).collect()[0]
    print(f"[processor/dvf] prix_m2 min={stats.prix_min:.0f} "
          f"max={stats.prix_max:.0f} moy={stats.prix_moy:.0f} | "
          f"{stats.nb_annees} années")

    _write_silver(df_clean, silver_path, "transactions", y, m, d)
    df_clean.unpersist()


# ─── Processor Délinquance ───────────────────────────────────────────────────

def process_delinquance(spark: SparkSession, bronze_path: str, silver_path: str,
                        y: str, m: str, d: str) -> None:
    """
    Nettoyage délinquance :
    - Filtre Paris (CODGEO_2025 IN 75101..75120)
    - Extraction arrondissement
    - Cast types numériques
    persist(MEMORY_AND_DISK) avant count() + écriture.
    """
    df_raw = _read_raw_partition(spark, bronze_path, "delinquance", y, m, d)

    df_clean = (
        df_raw
        .filter("CODGEO_2025 rlike '^7510[0-9]$|^75110$|^7511[0-9]$|^75120$'")
        .withColumn("arrondissement",
                    (F.col("CODGEO_2025").cast("int") - 75100).cast("smallint"))
        .withColumn("annee", F.col("annee").cast("smallint"))
        .withColumn("nombre", F.col("nombre").cast("double"))
        .withColumn("taux_pour_mille", F.col("taux_pour_mille").cast("double"))
        .select(
            "annee", "arrondissement", "CODGEO_2025",
            "indicateur", "nombre", "taux_pour_mille",
            "unite_de_compte", "est_diffuse",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nb = df_clean.count()
    nb_ind = df_clean.select("indicateur").distinct().count()
    print(f"[processor/delinquance] {nb:,} lignes Paris | {nb_ind} indicateurs")

    _write_silver(df_clean, silver_path, "delinquance", y, m, d)
    df_clean.unpersist()


# ─── Processor Logements sociaux ─────────────────────────────────────────────

def process_logements_sociaux(spark: SparkSession, bronze_path: str, silver_path: str,
                               y: str, m: str, d: str) -> None:
    """
    Nettoyage logements sociaux :
    - Cast types numériques (nb_logmt_total, nb_plai, nb_plus, nb_pls, annee)
    - Nettoyage arrdt → arrondissement int
    persist(MEMORY_AND_DISK) avant count() + écriture.
    """
    df_raw = _read_raw_partition(spark, bronze_path, "logements_sociaux", y, m, d)

    df_clean = (
        df_raw
        .withColumn("annee", F.col("annee").cast("smallint"))
        .withColumn("arrondissement", F.col("arrdt").cast("smallint"))
        .withColumn("nb_logmt_total", F.col("nb_logmt_total").cast("int"))
        .withColumn("nb_plai", F.col("nb_plai").cast("int"))
        .withColumn("nb_plus",
                    (F.coalesce(F.col("nb_plus").cast("int"), F.lit(0)) +
                     F.coalesce(F.col("nb_pluscd").cast("int"), F.lit(0))))
        .withColumn("nb_pls", F.col("nb_pls").cast("int"))
        .filter("arrondissement between 1 and 20")
        .select(
            "annee", "arrondissement", "adresse_programme",
            "code_postal", "nb_logmt_total", "nb_plai",
            "nb_plus", "nb_pls", "mode_real", "nature_programme",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nb = df_clean.count()
    total_logements = df_clean.agg(F.sum("nb_logmt_total")).collect()[0][0]
    print(f"[processor/logements_sociaux] {nb:,} programmes | {total_logements:,} logements total")

    _write_silver(df_clean, silver_path, "logements_sociaux", y, m, d)
    df_clean.unpersist()


# ─── Processor Revenus ───────────────────────────────────────────────────────

def process_revenus(spark: SparkSession, bronze_path: str, silver_path: str,
                    y: str, m: str, d: str) -> None:
    """
    Nettoyage revenus FiLoSoFi (BASE_TD_FILO_DEC_IRIS_2018.csv, sep=;) :
    - Filtre Paris : IRIS starts with '75' (codes 75101XXXX à 75120XXXX)
    - Extraction arrondissement depuis IRIS[3:5] (ex: '75101...' → 1)
    - Cast colonnes déciles et Gini (virgule → point → double)
    - Sélection des colonnes utiles uniquement
    persist(MEMORY_AND_DISK) avant count() + écriture.

    Note : le CSV ne contient pas COM, LIBIRIS, LIBCOM — uniquement la colonne IRIS.
    """
    df_raw = _read_raw_partition(spark, bronze_path, "revenus", y, m, d)

    # Colonnes numériques présentes dans le CSV FiLoSoFi
    numeric_cols = [
        "DEC_MED18", "DEC_Q118", "DEC_Q318", "DEC_GI18", "DEC_RD18",
        "DEC_D118", "DEC_D218", "DEC_D318", "DEC_D418", "DEC_D518",
        "DEC_D618", "DEC_D718", "DEC_D818", "DEC_D918",
        "DEC_PACT18", "DEC_PCHO18", "DEC_PPEN18",
    ]

    # Filtre Paris : IRIS codes 751010101 à 751200999
    df_paris = df_raw.filter("IRIS like '75%'")

    # Cast virgule → point puis double
    df_cast = df_paris
    for col_name in numeric_cols:
        if col_name in df_raw.columns:
            df_cast = df_cast.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name), ",", ".").cast("double")
            )

    # Extraction numéro arrondissement depuis IRIS (positions 3-4, base 1)
    # Ex: "751010101" → substring(4,2) = "01" → 1
    df_clean = (
        df_cast
        .withColumn(
            "arrondissement",
            F.substring(F.col("IRIS"), 4, 2).cast("smallint")
        )
        .select(
            "IRIS",
            "arrondissement",
            *[c for c in numeric_cols if c in df_raw.columns]
        )
        .filter("arrondissement between 1 and 20")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nb = df_clean.count()
    print(f"[processor/revenus] {nb:,} IRIS Paris après filtre (arrondissements 1–20)")

    _write_silver(df_clean, silver_path, "revenus", y, m, d)
    df_clean.unpersist()


# ─── Processor Arrondissements ───────────────────────────────────────────────

def process_arrondissements(spark: SparkSession, bronze_path: str, silver_path: str,
                             y: str, m: str, d: str) -> None:
    """
    Nettoie et type les données arrondissements depuis /raw :
    - arrondissement : smallint 1–20
    - superficie_km2 : double
    - population_totale : int
    persist(MEMORY_AND_DISK) : count() + écriture silver.
    """
    df_raw = _read_raw_partition(spark, bronze_path, "arrondissements", y, m, d)

    df_clean = (
        df_raw
        .withColumn("arrondissement", F.col("arrondissement").cast("smallint"))
        .withColumn("superficie_km2", F.col("superficie_km2").cast("double"))
        .withColumn("population_totale", F.col("population_totale").cast("int"))
        .filter("arrondissement between 1 and 20")
        # Calcul densité : hab/km² — arrondi à l'entier
        .withColumn(
            "densite_population",
            F.round(F.col("population_totale") / F.col("superficie_km2"), 0).cast("int")
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    nb = df_clean.count()
    df_clean.select("arrondissement", "superficie_km2", "population_totale", "densite_population") \
            .show(20, truncate=False)
    print(f"[processor/arrondissements] {nb} arrondissements — densité calculée")

    _write_silver(df_clean, silver_path, "arrondissements", y, m, d)
    df_clean.unpersist()


# ─── Résidences principales INSEE 2022 ───────────────────────────────────────

def process_residences_principales(spark: SparkSession, bronze_path: str, silver_path: str,
                                    y: str, m: str, d: str) -> None:
    """
    Nettoie résidences principales :
    - Cast CODGEO string, P22_RP double → int
    - Extraction arrondissement (CODGEO 75101 → 1)
    - Donnée de référence 2022 (pas de filtre annee)
    """
    df_raw = _read_raw_partition(spark, bronze_path, "residences_principales", y, m, d)

    df_clean = (
        df_raw
        .withColumn("arrondissement",
                    (F.col("CODGEO").cast("int") - 75100).cast("smallint"))
        .withColumn("nb_residences_principales",
                    F.col("P22_RP").cast("double").cast("int"))
        .select("CODGEO", "arrondissement", "nb_residences_principales")
        .filter("arrondissement between 1 and 20")
    )

    nb = df_clean.count()
    print(f"[processor/residences_principales] {nb} arrondissements Paris")

    _write_silver(df_clean, silver_path, "residences_principales", y, m, d)


# ─── Dispatcher ──────────────────────────────────────────────────────────────

PROCESSORS = {
    "dvf": process_dvf,
    "delinquance": process_delinquance,
    "logements_sociaux": process_logements_sociaux,
    "revenus": process_revenus,
    "arrondissements": process_arrondissements,
    "residences_principales": process_residences_principales,
}


def main() -> None:
    args = parse_args()
    dt = datetime.fromisoformat(args.date)
    y = str(dt.year)
    m = str(dt.month).zfill(2)
    d = str(dt.day).zfill(2)

    sources = list(PROCESSORS.keys()) if args.source == "all" else [args.source]

    spark = get_spark(f"UDE-Processor-{args.source}-{args.date}")
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  UDE Processor | source={args.source} | date={args.date}")
    print(f"  raw    : {args.bronze_path}")
    print(f"  silver : {args.silver_path}")
    print(f"{'='*60}\n")

    for source in sources:
        PROCESSORS[source](spark, args.bronze_path, args.silver_path, y, m, d)

    print(f"\n[processor] Terminé — {len(sources)} source(s) traitée(s).")
    spark.stop()


if __name__ == "__main__":
    main()
