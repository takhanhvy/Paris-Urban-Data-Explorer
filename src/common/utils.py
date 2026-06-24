"""
Utilitaires partagés : décorateur log_pipeline_run.
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

