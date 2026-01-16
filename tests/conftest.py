import pytest
import os
import sys
import allure

# Add framework to python path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from config.settings import settings
from framework.clients.bigquery import BigQueryClient
from framework.clients.triggers import DataflowTrigger


@pytest.fixture(scope="session")
def app_settings():
    return settings


@pytest.fixture(scope="session")
def bq_client(app_settings):
    """
    Returns a BigQueryClient.
    """
    return BigQueryClient(
        project_id=app_settings.project_id, location=app_settings.bq_location
    )

@pytest.fixture(scope="session")
def dataflow_trigger(app_settings):
    # Use region from config if available, fallback to us-central1
    region = app_settings.config.get("dataflow", {}).get("region", "us-central1")
    return DataflowTrigger(project_id=app_settings.project_id, region=region)

@pytest.fixture(scope="session")
def composer_trigger(app_settings):
    """
    Returns a ComposerTrigger.
    Uses environment details from config.
    """
    from framework.clients.triggers import ComposerTrigger
    return ComposerTrigger(
        project_id=app_settings.project_id, 
        location=app_settings.composer_location, 
        composer_env_name=app_settings.composer_env_name,
        webserver_url=app_settings.composer_webserver_url
    )

@pytest.fixture(scope="session")
def storage_client(app_settings):
    """Returns a StorageClient."""
    from framework.clients.storage import StorageClient
    return StorageClient(project_id=app_settings.project_id)



@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Allure hook to attach info on failure.
    """
    outcome = yield
    rep = outcome.get_result()
    
    if rep.when == "call" and rep.failed:
        # Check if the test has a 'query' attribute we can attach (dynamic attachment)
        if hasattr(item, "query"):
             allure.attach(
                item.query, 
                name="SQL Query", 
                attachment_type=allure.attachment_type.TEXT
            )
        
        # Attach last exception info
        if call.excinfo:
             allure.attach(
                str(call.excinfo), 
                name="Exception Info", 
                attachment_type=allure.attachment_type.TEXT
            )
