"""
Exceptions personnalisées Urban Data Explorer.
"""


class UDEBaseException(Exception):
    """Exception de base du projet."""


class IngestionError(UDEBaseException):
    """Erreur lors de l'ingestion d'une source."""


class TransformationError(UDEBaseException):
    """Erreur lors d'une transformation Silver ou Gold."""


class StorageError(UDEBaseException):
    """Erreur lors de l'écriture en base de données."""


class ConfigurationError(UDEBaseException):
    """Configuration manquante ou invalide."""


class APIError(UDEBaseException):
    """Erreur lors d'un appel à une API externe."""

    def __init__(self, message: str, status_code: int | None = None, url: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.url = url
