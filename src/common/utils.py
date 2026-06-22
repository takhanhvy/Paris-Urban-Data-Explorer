"""
Utilitaires partagés : décorateur pipeline_run, helpers geo, etc.
"""

import time
import functools
from datetime import datetime
from typing import Callable

from src.common.logger import get_logger

logger = get_logger(__name__)


def log_pipeline_run(pipeline_name: str, source: str, layer: str):
    """
    Décorateur qui loggue les métriques d'un pipeline dans ude.pipeline_runs.

    Usage:
        @log_pipeline_run("silver_dvf", "dvf", "silver")
        def process_dvf(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            started_at = datetime.utcnow()
            start_time = time.perf_counter()
            run_info = {
                "pipeline_name": pipeline_name,
                "source": source,
                "layer": layer,
                "started_at": started_at,
                "statut": "running",
            }
            logger.info(f"Pipeline démarré : {pipeline_name}", extra=run_info)

            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                logger.info(
                    f"Pipeline terminé : {pipeline_name}",
                    extra={**run_info, "statut": "success", "duration_s": round(duration, 3)},
                )
                # TODO Phase 2 : écrire dans ude.pipeline_runs via SQLAlchemy
                return result

            except Exception as e:
                duration = time.perf_counter() - start_time
                logger.error(
                    f"Pipeline échoué : {pipeline_name} — {e}",
                    extra={**run_info, "statut": "failed", "erreur": str(e), "duration_s": round(duration, 3)},
                )
                raise

        return wrapper
    return decorator


def insee_to_arrondissement(code_insee: str) -> int | None:
    """
    Convertit un code INSEE parisien en numéro d'arrondissement (1–20).

    Exemples :
        '75101' → 1
        '75120' → 20
        '75116' → 16
    """
    if not code_insee or len(code_insee) != 5 or not code_insee.startswith("751"):
        return None
    try:
        arr = int(code_insee[3:])
        return arr if 1 <= arr <= 20 else None
    except ValueError:
        return None


def code_postal_to_arrondissement(code_postal) -> int | None:
    """
    Convertit un code postal parisien en numéro d'arrondissement.

    Exemples :
        '75001' → 1
        '75016' → 16
    """
    import math
    if code_postal is None:
        return None
    # Gère les NaN (float) que Pandas peut passer
    if isinstance(code_postal, float) and math.isnan(code_postal):
        return None
    code_postal = str(code_postal).strip()
    if len(code_postal) != 5 or not code_postal.startswith("750"):
        return None
    try:
        arr = int(code_postal[3:])
        return arr if 1 <= arr <= 20 else None
    except ValueError:
        return None
