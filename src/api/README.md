# src/api — API FastAPI (C2.1)

API REST interopérable et sécurisée, point d'entrée du dashboard.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `main.py` | Application FastAPI, middleware CORS, auth API Key, tous les endpoints |
| `routers/` | (Phase 2) Découpage des endpoints par domaine |

## Endpoints

| Méthode | Route | Description |
|---------|-------|-------------|
| GET | `/health` | Health check (pas d'auth) |
| GET | `/arrondissements` | Liste des 20 arrondissements |
| GET | `/transactions` | Prix/m² par arrondissement × année |
| GET | `/logements-sociaux` | Stats logements sociaux |
| GET | `/delinquance` | Indicateurs délinquance |
| GET | `/revenus` | Données revenus par IRIS / arrondissement |
| GET | `/indicateurs/composite` | Table Gold complète |
| GET | `/qualite-air/live` | Qualité air temps réel (MongoDB) |

## Sécurité

- Header obligatoire : `X-API-Key: <votre_clé>`
- Validation des paramètres via Pydantic v2
- CORS configuré pour le dashboard JS

## Docs

- SwaggerUI : `http://localhost:8000/docs`
- ReDoc : `http://localhost:8000/redoc`

## Techno

`FastAPI 0.111`, `Pydantic v2`, `uvicorn`

## Lancement

```bash
uvicorn src.api.main:app --reload
```
