FROM python:3.11-slim

# ─── Dépendances système ─────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    gdal-bin \
    libgdal-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ─── Utilisateur non-root (bonne pratique sécurité) ──────────────────────────
RUN useradd -m -u 1000 appuser

WORKDIR /app

# ─── Dépendances Python (layer caché si requirements.txt inchangé) ───────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ─── Code source ─────────────────────────────────────────────────────────────
COPY --chown=appuser:appuser . .

# Dossiers de données writables par l'utilisateur
RUN mkdir -p data/raw data/bronze data/silver data/gold \
    && chown -R appuser:appuser data/

USER appuser

# ─── Healthcheck ─────────────────────────────────────────────────────────────
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

# ─── Démarrage (override dans docker-compose pour --reload en dev) ───────────
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
