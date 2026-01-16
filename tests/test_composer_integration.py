import pytest
import allure

@allure.feature("Orchestration")
@allure.story("Trigger Composer DAG")
@pytest.mark.integration
def test_trigger_composer_dag(composer_trigger):
    """
    Integration test to trigger a Composer DAG.
    This demonstrates the clean test automation approach.
    """
    dag_id = "sample_dag" # Replace with your actual DAG ID
    conf = {"test_execution": True}
    
    try:
        with allure.step(f"Triggering DAG: {dag_id}"):
            run_id = composer_trigger.trigger_job(dag_id, conf)
            allure.attach(str(run_id), name="DAG Run ID", attachment_type=allure.attachment_type.TEXT)
            
        assert run_id is not None
        print(f"Successfully triggered DAG run: {run_id}")
        
        # Example: Verify status (optional, requires real run_id)
        # status = composer_trigger.get_status(dag_id, run_id)
        # assert status['state'] in ['queued', 'running', 'success']
        
    except Exception as e:
        # In this generated environment without real creds/VPN, this is expected to fail.
        # We catch it to show the user what 'failure' looks like in a clean way, or re-raise.
        pytest.fail(f"Failed to trigger Composer DAG. Check credentials and URL. Error: {e}")
