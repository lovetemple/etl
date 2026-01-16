from google.cloud import bigquery
from typing import List, Dict, Any
import logging


class BigQueryClient:
    def __init__(self, project_id: str, location: str = "US"):
        self.client = bigquery.Client(project=project_id, location=location)
        self.project_id = project_id
        self.location = location
        self.logger = logging.getLogger(__name__)

    def execute_query(self, query: str, job_config=None) -> List[Dict[str, Any]]:
        """Executes a SQL query and returns results as a list of dicts."""
        self.logger.info(f"Executing query: {query}")
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()  # Waits for job to complete.
        return [dict(row) for row in results]

    def get_table(self, table_id: str):
        """Retrieves table metadata."""
        return self.client.get_table(table_id)

    def get_row_count(self, table_id: str) -> int:
        """Efficiently gets row count from table metadata."""
        table = self.get_table(table_id)
        return table.num_rows

    def check_table_exists(self, table_id: str) -> bool:
        """Checks if a table exists."""
        from google.cloud.exceptions import NotFound

        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            return False

    def insert_rows(self, table_id: str, rows: List[Dict[str, Any]]):
        """Inserts rows into a table (useful for test setup)."""
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise RuntimeError(f"Encountered errors while inserting rows: {errors}")

    def delete_table(self, table_id: str, not_found_ok: bool = True):
        """Deletes a table."""
        self.client.delete_table(table_id, not_found_ok=not_found_ok)
        self.logger.info(f"Deleted table {table_id}")
