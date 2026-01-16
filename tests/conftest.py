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
    return DataflowTrigger(project_id=app_settings.project_id, region="us-central1")

@pytest.fixture(scope="session")
def composer_trigger(app_settings):
    """
    Returns a ComposerTrigger.
    Currently a stub implementation for future Airflow integration.
    """
    from framework.clients.triggers import ComposerTrigger
    # Assumes composer environment details are in config or derivable, here using placeholder
    return ComposerTrigger(
        project_id=app_settings.project_id, 
        location="us-central1", 
        composer_env_name="test-env",
        webserver_url="https://example-airflow.composer.googleusercontent.com"
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
