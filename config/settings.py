import os
import yaml
from pathlib import Path
from typing import Any


class Settings:
    """
    Application settings loaded from environment variables and config.yaml.
    Priority: Env Vars > config.yaml
    """

    def __init__(self):
        from dotenv import load_dotenv
        
        # 1. Determine environment (default to dev)
        self.ENV: str = os.getenv("ETL_ENV", "dev")
        
        # 2. Load .env file based on environment
        env_specific = Path(__file__).parent.parent / f".env.{self.ENV}"
        if not env_specific.exists():
            raise FileNotFoundError(f"Missing environment file: {env_specific}. Every environment must have its own .env file.")
        
        load_dotenv(env_specific)

        # 3. Load variables (now populated from files if present)
        self.GCP_PROJECT: str | None = os.getenv("GCP_PROJECT")
        
        # Load config based on current environment (neighboring file)
        self._config_path = f"config.{self.ENV}.yaml"
        
        self.config: dict[str, Any] = {}
        self.load_config()

        # 4. Set simple attributes (no properties)
        # Priority: .env (GCP_PROJECT) > yaml root (project_id)
        self.project_id = self.GCP_PROJECT or self.config.get("project_id", "unknown-project")
        
        # Dataset Layers
        bq_conf = self.config.get("bigquery", {})
        self.raw_structured_ds = bq_conf.get("raw_structured", "raw_structured_ds")
        self.raw_vault_ds = bq_conf.get("raw_vault", "raw_vault_ds")
        self.business_vault_ds = bq_conf.get("business_vault", "business_vault_ds")
        self.consumption_ds = bq_conf.get("consumption", "consumption_ds")
        
        self.bq_location = bq_conf.get("location", "US")

        # Buckets
        buckets = self.config.get("buckets", {})
        self.landing_bucket = buckets.get("landing", "landing-bucket")
        self.temp_bucket = buckets.get("temp", "temp-bucket")

    def load_config(self):
        """Loads the YAML configuration file."""
        # Find config file in the same directory as this settings.py
        config_file = Path(__file__).parent / self._config_path
        if config_file.exists():
            with open(config_file, "r") as f:
                self.config = yaml.safe_load(f) or {}

            # Simple override logic if needed
            if self.ENV == "dev" and "environment" in self.config:
                pass
        else:
            print(f"Warning: Config file not found at {config_file}")


# Singleton instance
settings = Settings()
