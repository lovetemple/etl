import pytest
import allure

@allure.feature("Configuration")
@allure.story("Load Settings")
@pytest.mark.unit
def test_config_loading(app_settings):
    """Verify configuration loads correctly."""
    assert app_settings.ENV is not None
    # Check that settings can access config dict
    assert isinstance(app_settings.config, dict)

@allure.feature("BigQuery Integration")
@allure.story("Connection Check")
@pytest.mark.integration
def test_bq_connection(bq_client):
    """
    Simple integration test to check BigQuery connection.
    Requires valid credentials.
    """
    # This will fail if no credentials are provided, which is expected for real integration tests
    # We attempt a lightweight operation (listing datasets or similar, 
    # but our client wrapper doesn't have list_datasets yet, so just checking object init)
    with allure.step("Verify Project ID"):
        assert bq_client.project_id is not None
