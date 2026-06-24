"""
Dashboard Router (C2.1 + C1.1)

Endpoints publics (sans X-API-Key) pour le frontend JS.
Toutes les données viennent de ude.indicateurs_gold (PostgreSQL) et,
pour typologie/surfaces, du fichier Silver transactions_paris.parquet.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text

from src.common.config import get_settings
from src.common.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

router = APIRouter(prefix="/api", tags=["Dashboard"])

# ─── Singleton engine (dashboard) ────────────────────────────────────────────
_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            f"postgresql+pg8000://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
            pool_pre_ping=True,
        )
    return _engine


def _rows(result) -> list[dict]:
    return [dict(row._mapping) for row in result]


# ─── Constantes ───────────────────────────────────────────────────────────────

ARR_LABELS = {
    1: "1er arr. - Louvre",       2: "2ème arr. - Bourse",
    3: "3ème arr. - Temple",      4: "4ème arr. - Hôtel-de-Ville",
    5: "5ème arr. - Panthéon",    6: "6ème arr. - Luxembourg",
    7: "7ème arr. - Palais-Bourbon", 8: "8ème arr. - Élysée",
    9: "9ème arr. - Opéra",       10: "10ème arr. - Entrepôt",
    11: "11ème arr. - Popincourt", 12: "12ème arr. - Reuilly",
    13: "13ème arr. - Gobelins",  14: "14ème arr. - Observatoire",
    15: "15ème arr. - Vaugirard", 16: "16ème arr. - Passy",
    17: "17ème arr. - Batignolles", 18: "18ème arr. - Butte-Montmartre",
    19: "19ème arr. - Buttes-Chaumont", 20: "20ème arr. - Ménilmontant",
}

TYPOLOGY_SEGMENTS = ["studio_t1", "t2", "t3", "t4", "t5_plus"]
SURFACE_GROUPS = [
    ("lt_20",    lambda s: s < 20),
    ("bt_20_40", lambda s: 20 <= s < 40),
    ("bt_40_60", lambda s: 40 <= s < 60),
    ("bt_60_80", lambda s: 60 <= s < 80),
    ("bt_80_120",lambda s: 80 <= s < 120),
    ("gt_120",   lambda s: s >= 120),
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_arr(code: Optional[str]) -> Optional[int]:
    """Convertit '75101'→1, '75120'→20, 'all'→None."""
    if not code or code.strip().lower() == "all":
        return None
    try:
        n = int(code)
        if 75101 <= n <= 75120:
            return n - 75100
        if 1 <= n <= 20:
            return n
    except (ValueError, TypeError):
        pass
    return None


def _safe_float(v) -> Optional[float]:
    """Convertit en float Python, None si NaN/None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None


def _extract_air_label(doc: dict) -> Optional[str]:
    """
    Extrait le label de qualité de l'air depuis un document MongoDB.
    Structure stockée par feeder_airparif_batch :
      { "insee": "75101", "data": [ {"date": "...", "label": "Bon", ...}, ... ] }
    Gère aussi d'autres structures possibles (dict top-level, string).
    """
    if not doc:
        return None
    # Structure principale : data est une liste de forecasts
    data = doc.get("data")
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            return first.get("label") or first.get("qualif") or first.get("indice")
    # Fallback : clés top-level
    for key in ("label", "today", "indice", "qualif"):
        val = doc.get(key)
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            label = val.get("label") or val.get("qualif")
            if label:
                return label
    return None


def _get_air_quality(arr: Optional[int]) -> Optional[str]:
    """Qualité de l'air depuis MongoDB (best-effort, timeout 1s)."""
    try:
        from pymongo import MongoClient
        client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=1000)
        db = client[settings.mongo_db]
        collection = db["air_quality"]
        if arr is not None:
            insee = f"7510{arr}" if arr < 10 else f"751{arr}"
            doc = collection.find_one({"insee": insee}, {"_id": 0})
            client.close()
            return _extract_air_label(doc)
        else:
            docs = list(collection.find({}, {"_id": 0}).limit(5))
            client.close()
            for doc in docs:
                label = _extract_air_label(doc)
                if label:
                    return label
    except Exception:
        pass
    return None


def _load_silver_transactions() -> Optional[pd.DataFrame]:
    """Charge transactions_paris.parquet depuis data/silver/."""
    parquet_path = Path(settings.data_silver_path) / "transactions_paris.parquet"
    if not parquet_path.exists():
        return None
    df = pd.read_parquet(parquet_path)
    if "date_mutation" in df.columns:
        df["annee"] = pd.to_datetime(df["date_mutation"], errors="coerce").dt.year
    return df


def _classify_pieces(p) -> str:
    try:
        n = int(p) if p is not None else 0
    except (ValueError, TypeError):
        n = 0
    if n <= 1:
        return "studio_t1"
    elif n == 2:
        return "t2"
    elif n == 3:
        return "t3"
    elif n == 4:
        return "t4"
    else:
        return "t5_plus"


def _classify_surface(s) -> Optional[str]:
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    for seg_id, predicate in SURFACE_GROUPS:
        if predicate(v):
            return seg_id
    return None


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/arrondissements.geojson", include_in_schema=False)
async def serve_geojson():
    """Sert le GeoJSON des 20 arrondissements parisiens."""
    path = Path("dashboard/api/arrondissements.geojson")
    if not path.exists():
        raise HTTPException(status_code=404, detail="GeoJSON introuvable")
    return FileResponse(path, media_type="application/geo+json")


@router.get("/metrics")
async def get_metrics(
    year: int = Query(..., ge=2020, le=2025),
    arrondissement: Optional[str] = Query("all"),
):
    """
    KPI cards : prix/m², variation, logements sociaux, revenu médian,
    densité de population, qualité de l'air.
    """
    arr = _parse_arr(arrondissement)
    params: dict = {"year": year, "year_prev": year - 1}

    # Deux variantes du filtre : sans alias (pour CTEs) et avec alias g (pour main)
    if arr is not None:
        params["arr"] = arr
        arr_filter_cte  = "AND arrondissement = :arr"
        arr_filter_main = "AND g.arrondissement = :arr"
    else:
        arr_filter_cte  = ""
        arr_filter_main = ""

    sql = f"""
        WITH cumul_social AS (
            SELECT arrondissement,
                   SUM(COALESCE(nb_logements_sociaux, 0)) AS total_social
            FROM ude.indicateurs_gold
            WHERE annee <= :year
            GROUP BY arrondissement
        ),
        prev_year AS (
            SELECT arrondissement, prix_m2_median AS prev_prix
            FROM ude.indicateurs_gold
            WHERE annee = :year_prev
            {arr_filter_cte}
        )
        SELECT
            g.arrondissement,
            g.annee                             AS year,
            a.nom,
            g.prix_m2_median,
            g.nb_transactions                   AS transactions_total,
            g.revenu_median_arr                 AS revenu_median,
            g.score_attractivite,
            g.taux_delinquance_global,
            g.taux_delinquance_pmille,
            a.densite_population,
            CASE
                WHEN a.nb_residences_principales > 0
                THEN ROUND(
                    COALESCE(cs.total_social, 0) * 100.0
                    / NULLIF(a.nb_residences_principales::numeric, 0),
                    1
                )
                ELSE 0
            END                                 AS tx_logement_sociaux,
            CASE
                WHEN p.prev_prix > 0
                THEN ROUND(
                    ((g.prix_m2_median - p.prev_prix) / p.prev_prix * 100)::numeric,
                    2
                )
                ELSE NULL
            END                                 AS variation
        FROM ude.indicateurs_gold g
        JOIN ude.arrondissements a USING (arrondissement)
        LEFT JOIN cumul_social cs USING (arrondissement)
        LEFT JOIN prev_year p USING (arrondissement)
        WHERE g.annee = :year {arr_filter_main}
        ORDER BY g.arrondissement
    """

    with _get_engine().connect() as conn:
        rows = _rows(conn.execute(text(sql), params))

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée pour l'année {year}"
        )

    if arr is not None:
        row = rows[0]
        label = row.get("nom") or ARR_LABELS.get(arr, f"Arr. {arr}")
        result = {k: _safe_float(v) if isinstance(v, (int, float)) else v
                  for k, v in row.items()}
        result["label"] = label
        result["year"] = year
        result["air_quality_global"] = _get_air_quality(arr)
        return result
    else:
        # Agrégation Paris entier — moyenne pondérée par nb_transactions
        total_tx = sum(r["transactions_total"] or 0 for r in rows) or len(rows)

        def w_avg(field: str) -> Optional[float]:
            vals = [
                (_safe_float(r[field]), r["transactions_total"] or 1)
                for r in rows
                if r.get(field) is not None
            ]
            if not vals:
                return None
            num = sum(v * w for v, w in vals)
            den = sum(w for _, w in vals)
            return round(num / den, 2) if den else None

        def simple_avg(field: str) -> Optional[float]:
            vals = [_safe_float(r[field]) for r in rows if r.get(field) is not None]
            return round(sum(vals) / len(vals), 2) if vals else None

        return {
            "arrondissement": 0,
            "year": year,
            "nom": "Paris (tous arrondissements)",
            "label": "Paris (tous arrondissements)",
            "prix_m2_median": w_avg("prix_m2_median"),
            "variation": w_avg("variation"),
            "densite_population": simple_avg("densite_population"),
            "tx_logement_sociaux": simple_avg("tx_logement_sociaux"),
            "revenu_median": w_avg("revenu_median"),
            "transactions_total": total_tx,
            "score_attractivite": simple_avg("score_attractivite"),
            "taux_delinquance_global": simple_avg("taux_delinquance_global"),
            "taux_delinquance_pmille": simple_avg("taux_delinquance_pmille"),
            "air_quality_global": _get_air_quality(None),
        }


@router.get("/typology")
async def get_typology(
    year: int = Query(..., ge=2020, le=2025),
    arrondissement: Optional[str] = Query("all"),
):
    """
    Répartition par type de logement (Studios/T1 … T5+).
    Source : PostgreSQL ude.indicateurs_gold (colonnes part_studio_t1…part_t5_plus).
    """
    arr = _parse_arr(arrondissement)
    params: dict = {"year": year}
    arr_filter = "AND arrondissement = :arr" if arr is not None else ""
    if arr is not None:
        params["arr"] = arr

    sql = f"""
        SELECT
            AVG(nb_transactions)  AS total,
            AVG(part_studio_t1)   AS studio_t1,
            AVG(part_t2)          AS t2,
            AVG(part_t3)          AS t3,
            AVG(part_t4)          AS t4,
            AVG(part_t5_plus)     AS t5_plus
        FROM ude.indicateurs_gold
        WHERE annee = :year {arr_filter}
    """
    with _get_engine().connect() as conn:
        row = _rows(conn.execute(text(sql), params))

    if not row or row[0].get("studio_t1") is None:
        return {"segments": [], "source": "no_data"}

    r = row[0]
    total = _safe_float(r.get("total")) or 0
    segments = [
        {"id": "studio_t1", "value": _safe_float(r["studio_t1"]) or 0, "count": round(((_safe_float(r["studio_t1"]) or 0) / 100) * total)},
        {"id": "t2",        "value": _safe_float(r["t2"])        or 0, "count": round(((_safe_float(r["t2"])        or 0) / 100) * total)},
        {"id": "t3",        "value": _safe_float(r["t3"])        or 0, "count": round(((_safe_float(r["t3"])        or 0) / 100) * total)},
        {"id": "t4",        "value": _safe_float(r["t4"])        or 0, "count": round(((_safe_float(r["t4"])        or 0) / 100) * total)},
        {"id": "t5_plus",   "value": _safe_float(r["t5_plus"])   or 0, "count": round(((_safe_float(r["t5_plus"])   or 0) / 100) * total)},
    ]
    return {"segments": segments, "source": "gold", "total": int(total)}


@router.get("/surfaces")
async def get_surfaces(
    year: int = Query(..., ge=2020, le=2025),
    arrondissement: Optional[str] = Query("all"),
):
    """
    Répartition par tranche de surface (< 20 m² … > 120 m²).
    Source : PostgreSQL ude.indicateurs_gold (colonnes part_surf_*).
    """
    arr = _parse_arr(arrondissement)
    params: dict = {"year": year}
    arr_filter = "AND arrondissement = :arr" if arr is not None else ""
    if arr is not None:
        params["arr"] = arr

    sql = f"""
        SELECT
            AVG(nb_transactions)  AS total,
            AVG(part_surf_lt20)   AS lt_20,
            AVG(part_surf_20_40)  AS bt_20_40,
            AVG(part_surf_40_60)  AS bt_40_60,
            AVG(part_surf_60_80)  AS bt_60_80,
            AVG(part_surf_80_120) AS bt_80_120,
            AVG(part_surf_gt120)  AS gt_120
        FROM ude.indicateurs_gold
        WHERE annee = :year {arr_filter}
    """
    with _get_engine().connect() as conn:
        row = _rows(conn.execute(text(sql), params))

    if not row or row[0].get("lt_20") is None:
        return {"segments": [], "source": "no_data"}

    r = row[0]
    total = _safe_float(r.get("total")) or 0
    segments = [
        {"id": "lt_20",     "value": _safe_float(r["lt_20"])    or 0, "count": round(((_safe_float(r["lt_20"])    or 0) / 100) * total)},
        {"id": "bt_20_40",  "value": _safe_float(r["bt_20_40"]) or 0, "count": round(((_safe_float(r["bt_20_40"]) or 0) / 100) * total)},
        {"id": "bt_40_60",  "value": _safe_float(r["bt_40_60"]) or 0, "count": round(((_safe_float(r["bt_40_60"]) or 0) / 100) * total)},
        {"id": "bt_60_80",  "value": _safe_float(r["bt_60_80"]) or 0, "count": round(((_safe_float(r["bt_60_80"]) or 0) / 100) * total)},
        {"id": "bt_80_120", "value": _safe_float(r["bt_80_120"])or 0, "count": round(((_safe_float(r["bt_80_120"])or 0) / 100) * total)},
        {"id": "gt_120",    "value": _safe_float(r["gt_120"])   or 0, "count": round(((_safe_float(r["gt_120"])   or 0) / 100) * total)},
    ]
    return {"segments": segments, "source": "gold", "total": int(total)}


@router.get("/price/history")
async def get_price_history(
    arrondissement: Optional[str] = Query("all"),
):
    """
    Évolution du prix/m² médian de 2020 à 2025.
    Utilisé pour le graphique linéaire.
    """
    arr = _parse_arr(arrondissement)
    params: dict = {}

    if arr is not None:
        params["arr"] = arr
        arr_filter = "AND arrondissement = :arr"
    else:
        arr_filter = ""

    if arr is not None:
        sql = f"""
            SELECT annee AS year, prix_m2_median AS median_price_per_sqm
            FROM ude.indicateurs_gold
            WHERE prix_m2_median IS NOT NULL {arr_filter}
            ORDER BY annee
        """
    else:
        # Moyenne pondérée Paris entier
        sql = f"""
            SELECT
                annee AS year,
                ROUND(
                    SUM(prix_m2_median * COALESCE(nb_transactions, 1))::numeric
                    / NULLIF(SUM(COALESCE(nb_transactions, 1)), 0),
                    0
                ) AS median_price_per_sqm
            FROM ude.indicateurs_gold
            WHERE prix_m2_median IS NOT NULL {arr_filter}
            GROUP BY annee
            ORDER BY annee
        """

    with _get_engine().connect() as conn:
        rows = _rows(conn.execute(text(sql), params))

    prices = [
        {"year": r["year"], "median_price_per_sqm": _safe_float(r["median_price_per_sqm"])}
        for r in rows
        if r.get("median_price_per_sqm") is not None
    ]
    return {"prices": prices}


@router.get("/map/prices")
async def get_map_prices(
    year: int = Query(..., ge=2020, le=2025),
):
    """
    Prix/m² médian par arrondissement pour la carte choroplèthe.
    Retourne un dict {code_INSEE: prix} ex. {'75101': 12500, ...}
    """
    sql = """
        SELECT arrondissement, prix_m2_median
        FROM ude.indicateurs_gold
        WHERE annee = :year
        ORDER BY arrondissement
    """
    with _get_engine().connect() as conn:
        rows = _rows(conn.execute(text(sql), {"year": year}))

    prices = {}
    for row in rows:
        arr = row["arrondissement"]
        # code INSEE : 75101 → arr 1, ..., 75109 → arr 9, 75110 → arr 10, ..., 75120 → arr 20
        code = f"7510{arr}" if arr < 10 else f"751{arr}"
        prix = _safe_float(row["prix_m2_median"])
        if prix is not None:
            prices[code] = prix

    return {"year": year, "prices": prices}


@router.get("/data/table")
async def get_data_table(
    year: int = Query(..., ge=2020, le=2025),
):
    """
    Tableau récapitulatif pour l'onglet 'Données détaillées'.
    Retourne tous les arrondissements avec leurs indicateurs pour une année.
    """
    sql = """
        WITH prev AS (
            SELECT arrondissement, prix_m2_median AS prev_prix
            FROM ude.indicateurs_gold
            WHERE annee = :year_prev
        ),
        cumul_social AS (
            SELECT arrondissement,
                   SUM(COALESCE(nb_logements_sociaux, 0)) AS total_social
            FROM ude.indicateurs_gold
            GROUP BY arrondissement
        )
        SELECT
            g.arrondissement,
            a.nom,
            g.prix_m2_median,
            CASE
                WHEN p.prev_prix > 0
                THEN ROUND(
                    ((g.prix_m2_median - p.prev_prix) / p.prev_prix * 100)::numeric, 2
                )
                ELSE NULL
            END AS variation,
            g.nb_transactions,
            CASE
                WHEN a.population_2020 > 0
                THEN ROUND(
                    COALESCE(cs.total_social, 0) * 100.0
                    / NULLIF(a.population_2020::numeric / 2.2, 0), 1
                )
                ELSE 0
            END AS tx_logement_sociaux,
            g.revenu_median_arr,
            a.densite_population,
            g.score_attractivite
        FROM ude.indicateurs_gold g
        JOIN ude.arrondissements a USING (arrondissement)
        LEFT JOIN prev p USING (arrondissement)
        LEFT JOIN cumul_social cs USING (arrondissement)
        WHERE g.annee = :year
        ORDER BY g.arrondissement
    """
    with _get_engine().connect() as conn:
        rows = _rows(conn.execute(text(sql), {"year": year, "year_prev": year - 1}))

    data = []
    for row in rows:
        arr = row["arrondissement"]
        data.append({
            "arrondissement": arr,
            "nom": row.get("nom") or ARR_LABELS.get(arr, f"Arr. {arr}"),
            "prix_m2_median": _safe_float(row["prix_m2_median"]),
            "variation": _safe_float(row["variation"]),
            "nb_transactions": row.get("nb_transactions"),
            "tx_logement_sociaux": _safe_float(row["tx_logement_sociaux"]),
            "revenu_median_arr": _safe_float(row["revenu_median_arr"]),
            "densite_population": _safe_float(row["densite_population"]),
            "score_attractivite": _safe_float(row["score_attractivite"]),
        })

    return {"year": year, "data": data, "count": len(data)}
