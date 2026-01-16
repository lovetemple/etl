import logging
from typing import Dict, Any

class DataflowTrigger:
    def __init__(self, project_id: str, region: str):
        self.project_id = project_id
        self.region = region
        # Lazy load client to avoid dependency issues if not installed
        from googleapiclient.discovery import build

        self.dataflow = build("dataflow", "v1b3")
        self.logger = logging.getLogger(__name__)

    def trigger_job(
        self, template_path: str, job_name: str, parameters: Dict[str, Any] = None
    ):
        """Triggers a classic template Dataflow job."""
        self.logger.info(f"Triggering Dataflow job {job_name} from {template_path}")

        body = {
            "jobName": job_name,
            "parameters": parameters or {},
            "environment": {
                "tempLocation": f"gs://{parameters.get('temp_bucket', 'default')}/temp"
            },
        }

        request = self.dataflow.projects().locations().templates().launch(
            projectId=self.project_id,
            location=self.region,
            gcsPath=template_path,
            body=body
        )
        response = request.execute()
        job_id = response['job']['id']
        self.logger.info(f"Dataflow job triggered. ID: {job_id}")
        return job_id

    def get_status(self, job_id: str):
        request = (
            self.dataflow.projects()
            .locations()
            .jobs()
            .get(projectId=self.project_id, location=self.region, jobId=job_id)
        )
        return request.execute()


class ComposerTrigger:
    """
    Placeholder for Composer (Airflow) DAG triggering.
    Usually done via Cloud Composer API or making an HTTP request to the Airflow Webserver.
    """

    def __init__(self, project_id: str, location: str, composer_env_name: str, webserver_url: str):
        self.project_id = project_id
        self.location = location
        self.composer_env_name = composer_env_name
        self.webserver_url = webserver_url.rstrip("/")
        self.logger = logging.getLogger(__name__)

    def trigger_job(self, dag_id: str, conf: Dict[str, Any] = None):
        """
        Triggers a DAG run using the Airflow Stable REST API.
        URL: {webserver_url}/api/v1/dags/{dag_id}/dagRuns
        """
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
        import requests

        # Fetch ID token for the Composer webserver URL (IAP support)
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, self.webserver_url)
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        endpoint = f"{self.webserver_url}/api/v1/dags/{dag_id}/dagRuns"
        data = {
            "conf": conf or {}
        }
        
        self.logger.info(f"Triggering DAG {dag_id} at {endpoint}")
        response = requests.post(endpoint, json=data, headers=headers)
        
        if response.status_code != 200:
            raise RuntimeError(f"Failed to trigger DAG: {response.status_code} - {response.text}")
            
        return response.json().get("dag_run_id")

    def get_status(self, dag_id: str, dag_run_id: str):
        """
        Polls status of a DAG run.
        """
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
        import requests

        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, self.webserver_url)
        
        headers = {"Authorization": f"Bearer {token}"}
        endpoint = f"{self.webserver_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
             raise RuntimeError(f"Failed to get status: {response.status_code} - {response.text}")
             
        return response.json()
