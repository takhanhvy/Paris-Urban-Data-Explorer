# src/api — API FastAPI (C2.1)

API REST interopérable, point d'entrée du dashboard et des clients externes.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `main.py` | Application FastAPI, middleware CORS, health check, montage des routers |
| `routers/dashboard.py` | Tous les endpoints métier (arrondissements, métriques, qualité air) |

## Endpoints principaux

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/health` | Health check (pas d'auth) |
| GET | `/dashboard` | Dashboard HTML (MapLibre) |
| GET | `/api/metrics` | Métriques gold par arrondissement × année (PostgreSQL) |
| GET | `/api/arrondissements` | Géométries GeoJSON des 20 arrondissements (PostGIS) |
| GET | `/api/air-quality` | Qualité de l'air live (MongoDB) |

## Drivers

- PostgreSQL : `pg8000` (pur Python, pas de dépendance libpq)
- MongoDB : `pymongo`

## Sécurité

- CORS ouvert pour le dashboard JS (origines configurables via `.env`)
- Validation des paramètres via Pydantic v2

## Docs auto-générées

- SwaggerUI : `http://localhost:8000/docs`
- ReDoc : `http://localhost:8000/redoc`

## Techno

`FastAPI 0.111`, `Pydantic v2`, `uvicorn`, `pg8000`, `pymongo`

## Lancement (hors Docker)

```bash
uvicorn src.api.main:app --reload --port 8000
```

Via Docker (recommandé) :

```powershell
docker compose restart api
```
