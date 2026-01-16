from google.cloud import storage
import logging

class StorageClient:
    def __init__(self, project_id: str):
        self.client = storage.Client(project=project_id)
        self.logger = logging.getLogger(__name__)

    def upload_file(self, bucket_name: str, source_file_path: str, destination_blob_name: str):
        """Uploads a file to the bucket."""
        self.logger.info(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}")
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        self.logger.info("Upload complete.")

    def delete_blob(self, bucket_name: str, blob_name: str):
        """Deletes a blob from the bucket."""
        self.logger.info(f"Deleting gs://{bucket_name}/{blob_name}")
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.delete()
            self.logger.info("Deletion complete.")
        else:
            self.logger.warning(f"Blob {blob_name} does not exist.")

    def list_blobs(self, bucket_name: str, prefix: str | None = None):
        """Lists blobs in a bucket."""
        return list(self.client.list_blobs(bucket_name, prefix=prefix))
