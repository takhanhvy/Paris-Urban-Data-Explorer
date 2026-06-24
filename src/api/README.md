# src/api — API FastAPI (C2.1)

API REST du projet, point d'entrée unique pour le dashboard et les clients externes. Servie sur le port 8000 par le conteneur `ude_api`.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `main.py` | Application FastAPI : middleware CORS, auth X-API-Key, montage du router, connexions BDD (singletons), health check, montage des fichiers statiques du dashboard |
| `routers/dashboard.py` | Tous les endpoints métier |

## Endpoints

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/health` | Health check (sans auth) |
| GET | `/dashboard/` | Dashboard HTML (MapLibre GL JS) |
| GET | `/api/metrics` | Métriques gold par arrondissement × année — source : `ude.indicateurs_gold` (PostgreSQL) |
| GET | `/api/map/prices` | Prix médian/m² par arrondissement pour la carte choroplèthe |
| GET | `/api/typology` | Répartition typologique des transactions DVF (appartements, maisons, etc.) |
| GET | `/api/surfaces` | Distribution des surfaces par arrondissement |
| GET | `/api/price/history` | Évolution temporelle des prix 2020–2025 |
| GET | `/api/data/table` | Tableau de données complet pour le dashboard |

Les données de qualité de l'air proviennent de MongoDB (`urban_data_nosql.air_quality`) et sont injectées dans la réponse `/api/metrics` pour l'arrondissement demandé.

## Drivers BDD

- PostgreSQL : `pg8000` (pur Python, sans dépendance libpq) via SQLAlchemy 2.0 (`postgresql+pg8000://`)
- MongoDB : `pymongo`

## Sécurité

- Auth par header `X-API-Key` sur tous les endpoints (sauf `/health`)
- CORS configurable via `.env` (`ALLOWED_ORIGINS`)
- Validation stricte des paramètres via Pydantic v2

## Documentation auto-générée

- SwaggerUI : `http://localhost:8000/docs`
- ReDoc : `http://localhost:8000/redoc`

## Relancer l'API

```powershell
docker compose restart api
```
