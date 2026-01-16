# ETL Testing Framework

This reusable Python framework validates ETL pipelines on Google Cloud Platform (GCP), supporting multi-layer testing (Raw -> Consumption) and seamless environment management.

## 🏗️ Features

- **Environment Management**:
    - Support for `dev`, `test`, `staging` environments.
    - Automatic loading of `.env.{env}` files via `settings.py`.
- **E2E Pipeline Testing**:
    - Validate complex flows: GCS -> Composer -> Raw Structured -> Raw Vault -> Business Vault -> Consumption.
    - Support for **Initial (INI)** and **Change Data Capture (CDC)** load types.
    - **Data Seeding**: Upload test CSVs to GCS via `StorageClient`.
    - **Schema Consistency**: Automated checks between layers (e.g., Raw Vault vs Consumption).
- **Orchestration**:
    - **ComposerTrigger**: Trigger DAGs via Airflow Stable REST API with IAP authentication.
    - **DataflowTrigger**: Launch Classic/Flex templates.
- **Reporting & Observability**:
    - **Allure Reports**: Granular test steps, logging, and SQL query attachments.
    - **Pytest HTML**: Lightweight summary reports.
- **Developer Experience**:
    - **Manual Dispatch**: GitHub Actions workflow for parameterized runs (Environment, CSV selection, Stop-at-Layer debugging).
    - **Modern Tooling**: `uv` package management, `Ruff` linting, `Black` formatting.

## 🚀 Setup

1.  **Install uv**:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **Install Dependencies**:
    ```bash
    uv sync --dev
    ```

3.  **Configure Environment**:
    Set `ETL_ENV` to switch configurations (loads `.env.dev`, `.env.staging`, etc.):
    ```bash
    export ETL_ENV=dev
    ```

## 🧪 Running Tests

### Standard Execution
The framework is configured (via `pytest.ini`) to generate Allure results automatically.

```bash
uv run pytest
```

### End-to-End Pipeline Test
Run specific E2E scenarios for Initial or CDC loads:

```bash
uv run pytest tests/test_e2e_pipeline.py
```

### Viewing Reports
Serve the generated Allure report locally:

```bash
allure serve allure-results
```

### Manual Trigger (CI/CD)
Use the **"Manual E2E Test Dispatch"** workflow in GitHub Actions to:
- Select Target Environment (`dev`, `test`, `staging`).
- Upload specific Test Data (CSV filename).
- **Debug**: Stop execution after a specific layer (e.g., stop after `raw_vault`).

## 📂 Project Structure

- `config/`: Settings loader and environment-specific configs.
- `framework/clients/`: GCP wrappers (`bigquery.py`, `storage.py`, `triggers.py`).
- `framework/utils/`: Shared utilities (`assertions.py` with Allure steps).
- `tests/`: Test cases (`test_e2e_pipeline.py`, `test_composer_integration.py`).
- `.github/workflows/`: CI/CD definitions (`ci.yaml`, `manual_trigger.yaml`).
