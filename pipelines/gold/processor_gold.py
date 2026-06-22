"""
Processor Gold (C2.3 + C1.1)

Agrège les 4 sources Silver sur arrondissement × année et charge
dans ude.indicateurs_gold (PostgreSQL).

Sources :
  - data/silver/transactions_paris.parquet
  - data/silver/logements_sociaux.parquet
  - data/silver/delinquance_paris.parquet
  - data/silver/revenus_iris_paris.parquet
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging
from src.common.utils import log_pipeline_run

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

ANNEES = [2020, 2021, 2022, 2023, 2024, 2025]


# ─── Agrégation DVF ──────────────────────────────────────────────────────────

def _aggregate_dvf(silver_dir: Path) -> pd.DataFrame:
    f = silver_dir / "transactions_paris.parquet"
    if not f.exists():
        logger.warning("transactions_paris.parquet introuvable — DVF ignoré")
        return pd.DataFrame()

    df = pd.read_parquet(f)
    df["annee"] = pd.to_datetime(df["date_mutation"]).dt.year
    df = df[df["annee"].isin(ANNEES)]
    df = df[df["prix_m2"] > 0]

    agg = df.groupby(["arrondissement", "annee"]).agg(
        nb_transactions=("prix_m2", "count"),
        prix_m2_median=("prix_m2", "median"),
        prix_m2_moyen=("prix_m2", "mean"),
        prix_m2_q1=("prix_m2", lambda x: x.quantile(0.25)),
        prix_m2_q3=("prix_m2", lambda x: x.quantile(0.75)),
        surface_mediane=("surface_m2", "median"),
    ).reset_index()

    # Part des appartements
    appts = df[df["type_local"] == "Appartement"].groupby(["arrondissement", "annee"]).size().reset_index(name="nb_appts")
    agg = agg.merge(appts, on=["arrondissement", "annee"], how="left")
    agg["part_appartements"] = (agg["nb_appts"] / agg["nb_transactions"] * 100).round(2)

    # Arrondi des prix
    for col in ["prix_m2_median", "prix_m2_moyen", "prix_m2_q1", "prix_m2_q3"]:
        agg[col] = agg[col].round(2)

    return agg[["arrondissement", "annee", "nb_transactions", "prix_m2_median",
                "prix_m2_moyen", "prix_m2_q1", "prix_m2_q3", "surface_mediane", "part_appartements"]]


# ─── Agrégation Logements sociaux ────────────────────────────────────────────

def _aggregate_logements(silver_dir: Path) -> pd.DataFrame:
    f = silver_dir / "logements_sociaux.parquet"
    if not f.exists():
        logger.warning("logements_sociaux.parquet introuvable — ignoré")
        return pd.DataFrame()

    df = pd.read_parquet(f)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

    agg = df.groupby(["arrondissement", "annee"]).agg(
        nb_logements_sociaux=("nb_logmt_total", "sum"),
        nb_plai=("nb_plai", "sum"),
        nb_pls=("nb_pls", "sum"),
    ).reset_index()

    # nb_plus + nb_pluscd combinés
    if "nb_plus" in df.columns and "nb_pluscd" in df.columns:
        plus = df.groupby(["arrondissement", "annee"]).agg(
            nb_plus_pluscd=("nb_plus", "sum")
        ).reset_index()
        plus2 = df.groupby(["arrondissement", "annee"]).agg(
            nb_pluscd=("nb_pluscd", "sum")
        ).reset_index()
        agg = agg.merge(plus, on=["arrondissement", "annee"], how="left")
        agg = agg.merge(plus2, on=["arrondissement", "annee"], how="left")
        agg["nb_plus_pluscd"] = agg["nb_plus_pluscd"] + agg["nb_pluscd"].fillna(0)
        agg = agg.drop(columns=["nb_pluscd"])  # colonne intermédiaire, pas dans le DDL

    agg["annee"] = agg["annee"].astype(int)
    return agg


# ─── Agrégation Délinquance ───────────────────────────────────────────────────

def _aggregate_delinquance(silver_dir: Path) -> pd.DataFrame:
    f = silver_dir / "delinquance_paris.parquet"
    if not f.exists():
        logger.warning("delinquance_paris.parquet introuvable — ignoré")
        return pd.DataFrame()

    df = pd.read_parquet(f)
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

    # Identifier les cambriolages et violences dans l'indicateur
    camb_mask = df["indicateur"].str.contains("cambriolage|Cambriolage", na=False)
    viol_mask = df["indicateur"].str.contains("violence|Violence", na=False)

    camb = df[camb_mask].groupby(["arrondissement", "annee"])["taux_pour_mille"].mean().reset_index()
    camb = camb.rename(columns={"taux_pour_mille": "taux_cambriolages_pmille"})

    viol = df[viol_mask].groupby(["arrondissement", "annee"])["taux_pour_mille"].mean().reset_index()
    viol = viol.rename(columns={"taux_pour_mille": "taux_violences_pmille"})

    global_del = df.groupby(["arrondissement", "annee"])["taux_pour_mille"].mean().reset_index()
    global_del = global_del.rename(columns={"taux_pour_mille": "taux_delinquance_global"})

    agg = global_del.merge(camb, on=["arrondissement", "annee"], how="left")
    agg = agg.merge(viol, on=["arrondissement", "annee"], how="left")
    agg["annee"] = agg["annee"].astype(int)
    return agg


# ─── Agrégation Revenus ───────────────────────────────────────────────────────

def _aggregate_revenus(silver_dir: Path) -> pd.DataFrame:
    f = silver_dir / "revenus_iris_paris.parquet"
    if not f.exists():
        logger.warning("revenus_iris_paris.parquet introuvable — ignoré")
        return pd.DataFrame()

    df = pd.read_parquet(f)

    # Revenus 2018 : on les associe à toutes les années pour compléter la table Gold
    agg = df.groupby("arrondissement").agg(
        revenu_median_arr=("revenu_median", "median"),
        indice_gini_arr=("indice_gini", "mean"),
        rapport_interdecile_arr=("rapport_interdecile", "mean"),
    ).reset_index().round(4)

    return agg


# ─── Score attractivité composite ─────────────────────────────────────────────

def _compute_score(df: pd.DataFrame) -> pd.DataFrame:
    """Score 0–100 basé sur prix (inversé), revenus, délinquance (inversée)."""
    result = df.copy()

    def normalize_inv(s):
        """Normalise et inverse : valeur haute → score bas."""
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series([50.0] * len(s), index=s.index)
        return 100 - (s - mn) / (mx - mn) * 100

    def normalize(s):
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series([50.0] * len(s), index=s.index)
        return (s - mn) / (mx - mn) * 100

    score = pd.Series([50.0] * len(result), index=result.index)
    weights = 0

    if "prix_m2_median" in result.columns and result["prix_m2_median"].notna().any():
        score += normalize_inv(result["prix_m2_median"].fillna(result["prix_m2_median"].median())) * 0.4
        weights += 0.4

    if "revenu_median_arr" in result.columns and result["revenu_median_arr"].notna().any():
        score += normalize(result["revenu_median_arr"].fillna(result["revenu_median_arr"].median())) * 0.3
        weights += 0.3

    if "taux_delinquance_global" in result.columns and result["taux_delinquance_global"].notna().any():
        score += normalize_inv(result["taux_delinquance_global"].fillna(result["taux_delinquance_global"].median())) * 0.3
        weights += 0.3

    if weights > 0:
        result["score_attractivite"] = (score / (1 + weights)).clip(0, 100).round(2)
    else:
        result["score_attractivite"] = 50.0

    return result


# ─── Chargement PostgreSQL ────────────────────────────────────────────────────

def _load_to_postgres(df: pd.DataFrame, engine) -> None:
    """UPSERT dans ude.indicateurs_gold."""
    cols = df.columns.tolist()
    placeholders = ", ".join([f":{c}" for c in cols])
    col_names = ", ".join(cols)
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in ("arrondissement", "annee")])

    upsert_sql = f"""
        INSERT INTO ude.indicateurs_gold ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (arrondissement, annee) DO UPDATE SET {updates}, computed_at = NOW()
    """
    import math

    def _clean(v):
        """NaN → None, float entier (1.0) → int, float non-entier → float."""
        if isinstance(v, float):
            if math.isnan(v):
                return None
            if v == int(v):
                return int(v)
        return v

    records = [{k: _clean(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)
    logger.info(f"  {len(records)} lignes upsertées dans ude.indicateurs_gold")


# ─── Pipeline principal ───────────────────────────────────────────────────────

@log_pipeline_run("gold_aggregation", "all_sources", "gold")
def process_gold(silver_path: str | None = None) -> dict:
    silver_dir = Path(silver_path or settings.data_silver_path)

    logger.info("=== Pipeline Gold — agrégation des sources Silver ===")

    dvf = _aggregate_dvf(silver_dir)
    logements = _aggregate_logements(silver_dir)
    delinquance = _aggregate_delinquance(silver_dir)
    revenus = _aggregate_revenus(silver_dir)

    # Base : toutes les combinaisons arrondissement × année
    arrondissements = list(range(1, 21))
    base = pd.DataFrame(
        [(a, y) for a in arrondissements for y in ANNEES],
        columns=["arrondissement", "annee"],
    )

    gold = base.copy()

    if not dvf.empty:
        gold = gold.merge(dvf, on=["arrondissement", "annee"], how="left")
        logger.info(f"  DVF jointure OK")

    if not logements.empty:
        gold = gold.merge(logements, on=["arrondissement", "annee"], how="left")
        logger.info(f"  Logements jointure OK")

    if not delinquance.empty:
        gold = gold.merge(delinquance, on=["arrondissement", "annee"], how="left")
        logger.info(f"  Délinquance jointure OK")

    if not revenus.empty:
        gold = gold.merge(revenus, on="arrondissement", how="left")
        logger.info(f"  Revenus jointure OK")

    gold = _compute_score(gold)

    # Chargement PostgreSQL — pg8000 (driver Python pur, pas de libpq)
    engine = create_engine(
        f"postgresql+pg8000://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    _load_to_postgres(gold, engine)

    # Export Gold local (optionnel)
    gold_path = Path(settings.data_gold_path)
    gold_path.mkdir(parents=True, exist_ok=True)
    gold.to_parquet(gold_path / "indicateurs_gold.parquet", index=False)
    logger.info(f"Export Gold local : {gold_path / 'indicateurs_gold.parquet'}")

    logger.info(f"=== Pipeline Gold terminé : {len(gold)} lignes (20 arrondissements × {len(ANNEES)} années) ===")
    return {"nb_lignes_out": len(gold)}


if __name__ == "__main__":
    process_gold()
