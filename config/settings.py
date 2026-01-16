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
        # Using direct access [] instead of .get() to fail fast if missing
        self.project_id = self.GCP_PROJECT or self.config["project_id"]
        
        # Dataset Layers
        bq_conf = self.config["bigquery"]
        self.raw_structured_ds = bq_conf["raw_structured"]
        self.raw_vault_ds = bq_conf["raw_vault"]
        self.business_vault_ds = bq_conf["business_vault"]
        self.consumption_ds = bq_conf["consumption"]
        
        self.bq_location = bq_conf["location"]

        # Buckets
        buckets = self.config["buckets"]
        self.landing_bucket = buckets["landing"]
        self.temp_bucket = buckets["temp"]

        # Composer
        comp_conf = self.config["composer"]
        self.composer_location = comp_conf["location"]
        self.composer_env_name = comp_conf["env_name"]
        self.composer_webserver_url = comp_conf["webserver_url"]

    def load_config(self):
        """Loads the YAML configuration file."""
        # Find config file in the same directory as this settings.py
        config_file = Path(__file__).parent / self._config_path
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}. Every environment needs its own config.{self.ENV}.yaml")

        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f) or {}


# Singleton instance
settings = Settings()
