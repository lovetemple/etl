import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class Settings:
    """
    Application settings loaded from environment variables and config.yaml.
    Priority: Env Vars > config.yaml
    """

    def __init__(self):
        from dotenv import load_dotenv
        
        # 1. Determine environment (default to dev)
        self.ENV: str = os.getenv("ETL_ENV", "dev")
        
        # 2. Load .env files based on environment
        # Load .env.{ENV} first (e.g. .env.staging)
        env_specific = Path(__file__).parent.parent / f".env.{self.ENV}"
        load_dotenv(env_specific)
        
        # Load base .env as fallback
        base_env = Path(__file__).parent.parent / ".env"
        load_dotenv(base_env)

        # 3. Load variables (now populated from files if present)
        self.GCP_PROJECT: Optional[str] = os.getenv("GCP_PROJECT")
        self._config_path: str = os.getenv("ETL_CONFIG_PATH", "config/config.yaml")
        self.config: Dict[str, Any] = {}
        self.load_config()

        # 4. Set simple attributes (no properties)
        self.project_id = self.GCP_PROJECT or self.config.get("projects", {}).get(self.ENV, "unknown-project")
        
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
        # Assuming config is relative to the project root or this file
        # This settings.py is in config/, so parent is root/config, parent.parent is root
        config_file = Path(__file__).parent.parent / self._config_path
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
