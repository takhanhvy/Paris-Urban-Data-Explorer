"""
Urban Data Explorer — API FastAPI (C2.1)

Auth : header X-API-Key requis sur tous les endpoints (sauf /health).
Docs : /docs (SwaggerUI), /redoc
"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient
from sqlalchemy import create_engine, text

from src.api.routers import dashboard as dashboard_router
from src.common.config import get_settings
from src.common.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)
settings = get_settings()

# ─── Connexions BDD (singletons) ─────────────────────────────────────────────
_engine = None
_mongo_client = None
_mongo_db = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            f"postgresql+pg8000://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
            pool_pre_ping=True,
        )
    return _engine


def get_mongo_db():
    global _mongo_client, _mongo_db
    if _mongo_client is None:
        _mongo_client = MongoClient(settings.mongo_uri)
        _mongo_db = _mongo_client[settings.mongo_db]
    return _mongo_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API démarrée")
    yield
    if _mongo_client:
        _mongo_client.close()
    logger.info("API arrêtée")


# ─── Application ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="Urban Data Explorer API",
    description=(
        "Analyse du marché immobilier parisien : prix/m², logements sociaux, "
        "délinquance, revenus et qualité de l'air par arrondissement.\n\n"
        "**Auth :** header `X-API-Key` requis sur tous les endpoints."
    ),
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── Routers ──────────────────────────────────────────────────────────────────
app.include_router(dashboard_router.router)

# ─── Dashboard static files ───────────────────────────────────────────────────
_dashboard_path = Path("dashboard")
if _dashboard_path.exists():
    app.mount("/dashboard", StaticFiles(directory=str(_dashboard_path), html=True), name="dashboard")

# ─── Auth ─────────────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


async def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Clé API invalide")
    return api_key


AUTH = [Depends(verify_api_key)]


# ─── Helpers ──────────────────────────────────────────────────────────────────
def rows_to_list(result) -> list[dict]:
    return [dict(row._mapping) for row in result]


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Monitoring"])
async def health_check():
    return {"status": "ok", "version": app.version}


@app.get("/arrondissements", tags=["Référentiel"], dependencies=AUTH)
async def get_arrondissements():
    """Liste des 20 arrondissements parisiens (codes INSEE, codes postaux)."""
    with get_engine().connect() as conn:
        result = conn.execute(text(
            "SELECT arrondissement, code_insee, nom, code_postal, superficie_km2, population_2020 "
            "FROM ude.arrondissements ORDER BY arrondissement"
        ))
        return {"data": rows_to_list(result)}


@app.get("/transactions", tags=["Immobilier"], dependencies=AUTH)
async def get_transactions(
    arrondissement: Optional[int] = Query(None, ge=1, le=20),
    annee: Optional[int] = Query(None, ge=2020, le=2025),
    type_local: Optional[str] = Query(None, description="Appartement, Maison, etc."),
):
    """Prix/m² agrégés par arrondissement et année (depuis ude.indicateurs_gold)."""
    filters = []
    params = {}
    if arrondissement:
        filters.append("arrondissement = :arr")
        params["arr"] = arrondissement
    if annee:
        filters.append("annee = :annee")
        params["annee"] = annee

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT arrondissement, annee, nb_transactions, prix_m2_median,
               prix_m2_moyen, prix_m2_q1, prix_m2_q3, surface_mediane, part_appartements
        FROM ude.indicateurs_gold
        {where}
        ORDER BY arrondissement, annee
    """
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        return {"data": rows_to_list(result)}


@app.get("/logements-sociaux", tags=["Immobilier"], dependencies=AUTH)
async def get_logements_sociaux(
    arrondissement: Optional[int] = Query(None, ge=1, le=20),
    annee: Optional[int] = Query(None, ge=2000, le=2025),
):
    """Nombre de logements sociaux par type (PLAI, PLUS, PLS) et arrondissement."""
    filters = []
    params = {}
    if arrondissement:
        filters.append("arrondissement = :arr")
        params["arr"] = arrondissement
    if annee:
        filters.append("annee = :annee")
        params["annee"] = annee

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT arrondissement, annee, nb_logements_sociaux,
               nb_plai, nb_plus_pluscd, nb_pls
        FROM ude.indicateurs_gold
        {where}
        ORDER BY arrondissement, annee
    """
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        return {"data": rows_to_list(result)}


@app.get("/delinquance", tags=["Indicateurs"], dependencies=AUTH)
async def get_delinquance(
    arrondissement: Optional[int] = Query(None, ge=1, le=20),
    annee: Optional[int] = Query(None, ge=2020, le=2025),
):
    """Taux de délinquance par arrondissement et année."""
    filters = []
    params = {}
    if arrondissement:
        filters.append("arrondissement = :arr")
        params["arr"] = arrondissement
    if annee:
        filters.append("annee = :annee")
        params["annee"] = annee

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT arrondissement, annee,
               taux_cambriolages_pmille, taux_violences_pmille, taux_delinquance_global
        FROM ude.indicateurs_gold
        {where}
        ORDER BY arrondissement, annee
    """
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        return {"data": rows_to_list(result)}


@app.get("/revenus", tags=["Indicateurs"], dependencies=AUTH)
async def get_revenus(
    arrondissement: Optional[int] = Query(None, ge=1, le=20),
):
    """Revenus médians et indicateurs d'inégalité par arrondissement (données 2018)."""
    filters = []
    params = {}
    if arrondissement:
        filters.append("arrondissement = :arr")
        params["arr"] = arrondissement

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT DISTINCT arrondissement, revenu_median_arr, indice_gini_arr, rapport_interdecile_arr
        FROM ude.indicateurs_gold
        {where}
        ORDER BY arrondissement
    """
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        return {"data": rows_to_list(result)}


@app.get("/indicateurs/composite", tags=["Analytique"], dependencies=AUTH)
async def get_indicateurs_composite(
    annee: Optional[int] = Query(None, ge=2020, le=2025),
):
    """
    Tous les indicateurs agrégés par arrondissement × année.
    Endpoint principal du dashboard (carte choroplèthe).
    """
    params = {}
    where = ""
    if annee:
        where = "WHERE annee = :annee"
        params["annee"] = annee

    sql = f"""
        SELECT ig.*, a.nom, a.code_postal, a.superficie_km2
        FROM ude.indicateurs_gold ig
        JOIN ude.arrondissements a USING (arrondissement)
        {where}
        ORDER BY ig.arrondissement, ig.annee
    """
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params)
        return {"data": rows_to_list(result)}


@app.get("/qualite-air/live", tags=["Temps réel"], dependencies=AUTH)
async def get_qualite_air(
    arrondissement: Optional[int] = Query(None, ge=1, le=20),
):
    """
    Qualité de l'air en temps réel depuis MongoDB (mise à jour toutes les heures).
    Les données expirent automatiquement après 24h (index TTL).
    """
    db = get_mongo_db()
    collection = db["air_quality"]

    query = {}
    if arrondissement:
        # Code INSEE : 75101 → arr 1, 75120 → arr 20
        insee = f"7510{arrondissement}" if arrondissement < 10 else f"751{arrondissement}"
        query["insee"] = insee

    docs = list(collection.find(query, {"_id": 0}).sort("insee", 1))

    if not docs:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée de qualité de l'air disponible. "
                   "Lancer : python -m ingestion.api.feeder_airparif_batch",
        )

    return {"data": docs, "count": len(docs)}
