"""
Configuration centralisée via pydantic-settings.
Charge les variables depuis .env + config/dev.yaml.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "urban_data"
    postgres_user: str = "ude_user"
    postgres_password: str = "changeme"

    @property
    def postgres_dsn(self):
        from sqlalchemy.engine import URL
        return URL.create(
            drivername="postgresql",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )

    # MongoDB
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "urban_data_nosql"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_airparif: str = "airparif.quality"

    # API Airparif
    airparif_api_key: str = Field(default="", alias="AIRPARIF_API_KEY")
    airparif_base_url: str = "https://api.airparif.fr/indices/prevision/commune"

    # API Paris OpenData
    paris_opendata_url: str = (
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
        "/logements-sociaux-finances-a-paris/records"
    )

    # API interne
    api_key: str = "changeme-api-key"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True

    # Chemins données
    data_raw_path: str = "data/raw"
    data_bronze_path: str = "data/bronze"
    data_silver_path: str = "data/silver"
    data_gold_path: str = "data/gold"

    # Arrondissements (codes INSEE)
    arrondissements_insee: list[str] = [
        f"7510{i}" if i < 10 else f"751{i}" for i in range(1, 21)
    ]


@lru_cache
def get_settings() -> Settings:
    """Singleton settings — utiliser dans toute l'appli."""
    return Settings()
