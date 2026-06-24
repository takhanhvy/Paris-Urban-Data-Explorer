"""
datamart.py — Spark job : HDFS /silver → PostgreSQL datamarts (ude.indicateurs_gold)

Architecture : hdfs://namenode:9000/urban-data/silver/ → PostgreSQL ude.indicateurs_gold

Paramètres :
  --silver-path   Chemin HDFS /silver (ex: hdfs://namenode:9000/urban-data/silver)
  --jdbc-url      JDBC URL PostgreSQL
  --jdbc-user     Utilisateur PostgreSQL
  --jdbc-password Mot de passe PostgreSQL
  --jdbc-table    Table cible (défaut: ude.indicateurs_gold)
  --annee         Année à agréger (défaut: toutes)
  --write-mode    overwrite | append (défaut: overwrite)

Optimisation Spark :
  cache() sur chaque source silver après lecture — chaque DataFrame est réutilisé
  pour plusieurs groupBy (prix, surfaces, transactions). Visible dans Spark UI Storage.

Usage (depuis le conteneur spark-master du cluster Marcel-Jan) :
  docker exec spark-master /opt/spark-apps/submit/datamart.sh 2026-06-24

  Ou directement :
  spark-submit --master spark://spark-master:7077 \\
    --name "UDE-Datamart" \\
    --packages "org.postgresql:postgresql:42.7.3" \\
    /opt/spark-apps/datamart.py \\
    --silver-path hdfs://namenode:9000/urban-data/silver \\
    --jdbc-url "jdbc:postgresql://host.docker.internal:5433/urban_data" \\
    --jdbc-user ude_user \\
    --jdbc-password changeme

Note : host.docker.internal:5433 pointe vers notre PostgreSQL depuis le réseau Marcel-Jan.
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ─── Session Spark ────────────────────────────────────────────────────────────

def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ─── Parseur d'arguments ─────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="UDE Datamart — /silver → PostgreSQL")
    p.add_argument("--silver-path", required=True,
                   help="Chemin HDFS /silver (ex: hdfs://namenode:9000/urban-data/silver)")
    p.add_argument("--jdbc-url", required=True,
                   help="JDBC URL (ex: jdbc:postgresql://postgres:5432/urban_data)")
    p.add_argument("--jdbc-user", required=True)
    p.add_argument("--jdbc-password", required=True)
    p.add_argument("--jdbc-table", default="ude.indicateurs_gold",
                   help="Table PostgreSQL cible")
    p.add_argument("--annee", type=int, default=None,
                   help="Filtrer sur une année spécifique (défaut: toutes)")
    p.add_argument("--write-mode", choices=["overwrite", "append"], default="overwrite")
    return p.parse_args()


# ─── Lecture silver avec cache ────────────────────────────────────────────────

def _read_silver(spark: SparkSession, silver_path: str, source: str,
                 annee: int = None) -> DataFrame:
    """
    Lit une source silver depuis HDFS.
    cache() : chaque DataFrame sera réutilisé pour plusieurs agrégations.
    Visible dans Spark UI > Storage tab.
    """
    df = spark.read.parquet(f"{silver_path}/{source}/")
    if annee and "annee" in df.columns:
        df = df.filter(F.col("annee") == annee)
    df = df.cache()
    nb = df.count()  # déclenche le cache
    print(f"[datamart] Silver/{source} : {nb:,} lignes (cache activé)")
    return df


# ─── Agrégation DVF ──────────────────────────────────────────────────────────

def _agg_dvf(df_dvf: DataFrame) -> DataFrame:
    """
    Agrège les transactions par arrondissement × annee.
    Calcule : nb_transactions, prix_m2_median, prix_m2_moyen, Q1, Q3,
              surface_mediane, part_appartements,
              typologies (studio_t1…t5_plus), tranches de surface.
    Le DataFrame df_dvf est déjà en cache → tous les groupBy réutilisent le cache.
    """
    # Agrégats principaux
    agg_main = df_dvf.groupBy("arrondissement", "annee").agg(
        F.count("*").alias("nb_transactions"),
        F.percentile_approx("prix_m2", 0.5, 1000).alias("prix_m2_median"),
        F.round(F.avg("prix_m2"), 2).alias("prix_m2_moyen"),
        F.percentile_approx("prix_m2", 0.25, 1000).alias("prix_m2_q1"),
        F.percentile_approx("prix_m2", 0.75, 1000).alias("prix_m2_q3"),
        F.percentile_approx("surface_reelle_bati", 0.5, 1000).alias("surface_mediane"),
        # Typologie par nombre de pièces (% sur toutes transactions)
        F.round(F.avg(F.when(F.col("nombre_pieces_principales") == 1, 1).otherwise(0)) * 100, 2).alias("part_studio_t1"),
        F.round(F.avg(F.when(F.col("nombre_pieces_principales") == 2, 1).otherwise(0)) * 100, 2).alias("part_t2"),
        F.round(F.avg(F.when(F.col("nombre_pieces_principales") == 3, 1).otherwise(0)) * 100, 2).alias("part_t3"),
        F.round(F.avg(F.when(F.col("nombre_pieces_principales") == 4, 1).otherwise(0)) * 100, 2).alias("part_t4"),
        F.round(F.avg(F.when(F.col("nombre_pieces_principales") >= 5, 1).otherwise(0)) * 100, 2).alias("part_t5_plus"),
        # Tranches de surface (%)
        F.round(F.avg(F.when(F.col("surface_reelle_bati") < 20, 1).otherwise(0)) * 100, 2).alias("part_surf_lt20"),
        F.round(F.avg(F.when((F.col("surface_reelle_bati") >= 20) & (F.col("surface_reelle_bati") < 40), 1).otherwise(0)) * 100, 2).alias("part_surf_20_40"),
        F.round(F.avg(F.when((F.col("surface_reelle_bati") >= 40) & (F.col("surface_reelle_bati") < 60), 1).otherwise(0)) * 100, 2).alias("part_surf_40_60"),
        F.round(F.avg(F.when((F.col("surface_reelle_bati") >= 60) & (F.col("surface_reelle_bati") < 80), 1).otherwise(0)) * 100, 2).alias("part_surf_60_80"),
        F.round(F.avg(F.when((F.col("surface_reelle_bati") >= 80) & (F.col("surface_reelle_bati") < 120), 1).otherwise(0)) * 100, 2).alias("part_surf_80_120"),
        F.round(F.avg(F.when(F.col("surface_reelle_bati") >= 120, 1).otherwise(0)) * 100, 2).alias("part_surf_gt120"),
    )

    # Part appartements (utilise le cache — 2e groupBy)
    total = df_dvf.groupBy("arrondissement", "annee").agg(
        F.count("*").alias("total")
    )
    appts = df_dvf.filter(F.col("type_local") == "Appartement") \
                  .groupBy("arrondissement", "annee") \
                  .agg(F.count("*").alias("nb_appts"))

    part_appts = (
        total.join(appts, ["arrondissement", "annee"], "left")
        .withColumn("part_appartements",
                    F.round(F.col("nb_appts") / F.col("total") * 100, 2))
        .select("arrondissement", "annee", "part_appartements")
    )

    return agg_main.join(part_appts, ["arrondissement", "annee"], "left")


# ─── Agrégation Logements sociaux ────────────────────────────────────────────

def _agg_logements(df_log: DataFrame) -> DataFrame:
    """Agrège les logements sociaux par arrondissement × annee."""
    return df_log.groupBy("arrondissement", "annee").agg(
        F.sum("nb_logmt_total").alias("nb_logements_sociaux"),
        F.sum("nb_plai").alias("nb_plai"),
        F.sum("nb_plus").alias("nb_plus_pluscd"),
        F.sum("nb_pls").alias("nb_pls"),
    )


# ─── Agrégation Délinquance ───────────────────────────────────────────────────

def _agg_delinquance(df_del: DataFrame) -> DataFrame:
    """
    Pivot délinquance : extrait cambriolages et violences par arrondissement × annee.
    Utilise le cache → 2 filter() sur le même DataFrame.
    """
    cambriolages = (
        df_del
        .filter(F.col("indicateur").contains("cambriolage"))
        .groupBy("arrondissement", "annee")
        .agg(F.sum("taux_pour_mille").alias("taux_cambriolages_pmille"))
    )

    violences = (
        df_del
        .filter(F.col("indicateur").contains("violence") |
                F.col("indicateur").contains("coups"))
        .groupBy("arrondissement", "annee")
        .agg(F.sum("taux_pour_mille").alias("taux_violences_pmille"))
    )

    global_del = (
        df_del
        .groupBy("arrondissement", "annee")
        .agg(
            F.sum("nombre").cast("integer").alias("taux_delinquance_global"),
            F.round(F.sum("taux_pour_mille"), 1).alias("taux_delinquance_pmille"),
        )
    )

    return (
        global_del
        .join(cambriolages, ["arrondissement", "annee"], "left")
        .join(violences, ["arrondissement", "annee"], "left")
    )


# ─── Agrégation Revenus (IRIS → arrondissement) ──────────────────────────────

def _agg_revenus(df_rev: DataFrame) -> DataFrame:
    """
    Agrège les revenus IRIS → arrondissement.
    La colonne arrondissement (1–20) est déjà calculée par processor.py
    via substring(IRIS, 4, 2) — on l'utilise directement sans re-dériver.
    """
    return (
        df_rev
        .filter(F.col("arrondissement").between(1, 20))
        .groupBy("arrondissement")
        .agg(
            F.round(F.avg("DEC_MED18"), 2).alias("revenu_median_arr"),
            F.round(F.avg("DEC_GI18"), 4).alias("indice_gini_arr"),
            F.round(F.avg("DEC_RD18"), 2).alias("rapport_interdecile_arr"),
        )
    )


# ─── Score attractivité ───────────────────────────────────────────────────────

def _compute_score(df: DataFrame) -> DataFrame:
    """
    Score attractivité 0–100 (business rule normalisée Min-Max) :
      + revenus élevés → score+
      + peu de délinquance → score+
      + logements sociaux (diversité) → score+ (jusqu'à 20%, puis neutre)
      - prix élevés → score- (accessibilité)
    """
    # Normalisation min-max sur la fenêtre complète
    w = Window.partitionBy("annee")

    def minmax(col_name, inverse=False):
        mn = F.min(col_name).over(w)
        mx = F.max(col_name).over(w)
        norm = (F.col(col_name) - mn) / (mx - mn + F.lit(1e-9))
        return (F.lit(1) - norm) if inverse else norm

    return (
        df
        .withColumn("_s_revenu",       minmax("revenu_median_arr"))
        .withColumn("_s_del",          minmax("taux_delinquance_global", inverse=True))
        .withColumn("_s_prix",         minmax("prix_m2_median",          inverse=True))
        .withColumn("_s_logements",    F.least(
            F.coalesce(F.col("nb_logements_sociaux"), F.lit(0)).cast("double") /
            (F.coalesce(F.col("nb_transactions"), F.lit(1)).cast("double") * 5 + 1),
            F.lit(1.0)
        ))
        .withColumn(
            "score_attractivite",
            F.round(
                (F.col("_s_revenu") * 35 +
                 F.col("_s_del")    * 30 +
                 F.col("_s_prix")   * 25 +
                 F.col("_s_logements") * 10),
                2
            )
        )
        .drop("_s_revenu", "_s_del", "_s_prix", "_s_logements")
    )


# ─── Écriture JDBC ────────────────────────────────────────────────────────────

def _write_jdbc(df: DataFrame, jdbc_url: str, user: str, password: str,
                table: str, mode: str) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .mode(mode)
        .save()
    )
    print(f"[datamart] ✓ {df.count():,} lignes écrites dans {table} (mode={mode})")


# ─── Mise à jour dimension arrondissements ────────────────────────────────────

def _update_arrondissements_dim(df_arr: DataFrame, jdbc_url: str,
                                 user: str, password: str) -> None:
    """
    Met à jour ude.arrondissements (superficie_km2 + population_2020) depuis silver.
    20 lignes seulement → collecte côté driver + UPDATE via le driver JDBC PostgreSQL
    déjà présent dans le classpath Spark (--packages org.postgresql:postgresql:42.7.3).
    Pas besoin de pg8000 ni de reconstruire l'image.
    """
    from py4j.java_gateway import java_import

    rows = df_arr.select(
        "arrondissement", "superficie_km2", "population_totale", "densite_population"
    ).collect()

    # Utilise le driver JDBC PostgreSQL via le JVM bridge PySpark.
    # DriverManager.getConnection() échoue car le driver est dans le classloader
    # Spark, pas le classloader système. On instancie org.postgresql.Driver
    # directement pour contourner ce problème.
    jvm = df_arr.sparkSession.sparkContext._jvm
    java_import(jvm, "java.util.Properties")

    props = jvm.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    driver = jvm.org.postgresql.Driver()
    conn = driver.connect(jdbc_url, props)
    try:
        stmt = conn.createStatement()
        for row in rows:
            sql = (
                f"UPDATE ude.arrondissements "
                f"SET superficie_km2 = {float(row.superficie_km2)}, "
                f"    population_2020 = {int(row.population_totale)}, "
                f"    densite_population = {int(row.densite_population)} "
                f"WHERE arrondissement = {int(row.arrondissement)}"
            )
            stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()

    print(f"[datamart/arrondissements] ✓ {len(rows)} arrondissements mis à jour (superficie + population + densité)")


def _update_residences_principales(df_rp: DataFrame, jdbc_url: str,
                                    user: str, password: str) -> None:
    """
    Met à jour ude.arrondissements.nb_residences_principales depuis silver.
    Source : base-cc-logement-2022.CSV (INSEE RP 2022), 20 lignes.
    """
    from py4j.java_gateway import java_import

    rows = df_rp.select("arrondissement", "nb_residences_principales").collect()

    jvm = df_rp.sparkSession.sparkContext._jvm
    java_import(jvm, "java.util.Properties")
    props = jvm.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    driver = jvm.org.postgresql.Driver()
    conn = driver.connect(jdbc_url, props)
    try:
        stmt = conn.createStatement()
        for row in rows:
            sql = (
                f"UPDATE ude.arrondissements "
                f"SET nb_residences_principales = {int(row.nb_residences_principales)} "
                f"WHERE arrondissement = {int(row.arrondissement)}"
            )
            stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()

    print(f"[datamart/residences_principales] ✓ {len(rows)} arrondissements mis à jour")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    spark = get_spark(f"UDE-Datamart-{'all' if not args.annee else args.annee}")
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  UDE Datamart | annee={args.annee or 'toutes'}")
    print(f"  silver : {args.silver_path}")
    print(f"  jdbc   : {args.jdbc_url} → {args.jdbc_table}")
    print(f"{'='*60}\n")

    # ── Dimension arrondissements → UPDATE ude.arrondissements ────────────────
    df_arr = _read_silver(spark, args.silver_path, "arrondissements")
    _update_arrondissements_dim(df_arr, args.jdbc_url, args.jdbc_user, args.jdbc_password)
    df_arr.unpersist()

    df_rp = _read_silver(spark, args.silver_path, "residences_principales")
    _update_residences_principales(df_rp, args.jdbc_url, args.jdbc_user, args.jdbc_password)
    df_rp.unpersist()

    # ── Lecture silver avec cache (réutilisé pour agrégations multiples) ──────
    df_dvf = _read_silver(spark, args.silver_path, "transactions", args.annee)
    df_log = _read_silver(spark, args.silver_path, "logements_sociaux", args.annee)
    df_del = _read_silver(spark, args.silver_path, "delinquance", args.annee)
    df_rev = _read_silver(spark, args.silver_path, "revenus")  # pas d'annee (2018)

    # ── Agrégations (chaque source en cache → groupBy multiples sans re-lecture)
    print("\n[datamart] Calcul des agrégats...")
    agg_dvf = _agg_dvf(df_dvf)
    agg_log = _agg_logements(df_log)
    agg_del = _agg_delinquance(df_del)
    agg_rev = _agg_revenus(df_rev)

    # ── Jointure toutes sources sur arrondissement × annee ────────────────────
    df_gold = (
        agg_dvf
        .join(agg_log, ["arrondissement", "annee"], "left")
        .join(agg_del, ["arrondissement", "annee"], "left")
        .join(agg_rev, ["arrondissement"],           "left")
    )

    # ── Score attractivité (business rule) ────────────────────────────────────
    df_gold = _compute_score(df_gold)

    # ── Libérer les caches sources ────────────────────────────────────────────
    df_dvf.unpersist()
    df_log.unpersist()
    df_del.unpersist()
    df_rev.unpersist()

    nb_final = df_gold.count()
    print(f"\n[datamart] {nb_final} lignes gold (arrondissements × années)")

    # ── Écriture JDBC → PostgreSQL ────────────────────────────────────────────
    _write_jdbc(
        df_gold, args.jdbc_url, args.jdbc_user, args.jdbc_password,
        args.jdbc_table, args.write_mode
    )

    print("\n[datamart] Pipeline terminé.")
    spark.stop()


if __name__ == "__main__":
    main()
