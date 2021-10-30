from azure.storage.blob import BlobServiceClient
import os
import logging


class BlobStorageClient:

    def __init__(self, container_name: str, blob_name: str) -> None:

        self.blob_client_object = BlobServiceClient.from_connection_string(
            os.environ.get("BLOB_STORAGE_CONNECTION_STRING"))

        self.container_client = self.blob_client_object.get_container_client(
            container_name)

        self.blob_client = self.container_client.get_blob_client(blob_name)

        logging.info("Connected to blob storage successfully")

    @property
    def get_blob_client(self):
        return self.blob_client
