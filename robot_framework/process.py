"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
from io import BytesIO
import requests
from azure.storage.blob import BlobServiceClient
import json

# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
     
    orchestrator_connection.log_trace("Running process.")

    # 1. Parse Queue Data
    data = json.loads(queue_element.data)
    case_id = data.get("tilsyn_id")
    blob_path = data.get("blob_path")
    filename = data.get("filename")
    
    # 2. Get Credentials
    vejman_token = orchestrator_connection.get_credential("VejmanToken").password
    azure_cred = orchestrator_connection.get_credential("TilsynBlobConnection")
    azure_container = azure_cred.username
    azure_connection_string = azure_cred.password

    # 3. Download from Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    blob_client = blob_service_client.get_blob_client(container=azure_container, blob=blob_path)
    
    orchestrator_connection.log_trace(f"Downloading {blob_path} from Azure.")
    blob_data = blob_client.download_blob().readall()
    file_stream = BytesIO(blob_data)

    # 4. Upload to Vejman
    url = f"https://vejman.vd.dk/permissions/file?token={vejman_token}"

    payload = {
        'caseid': case_id,
        'type': '4',
        'transaction': 'undefined'
    }
    
    files = [
        ('filename', (filename, file_stream, 'image/jpeg'))
    ]
    
    headers = {
        'accept': 'application/json',
        'x-requested-with': 'XMLHttpRequest'
    }

    orchestrator_connection.log_trace(f"Uploading {filename} to Vejman for case {case_id}.")
    response = requests.post(url, headers=headers, data=payload, files=files, timeout=30)
    response.raise_for_status()

    orchestrator_connection.log_info(f"Successfully processed {filename}: {response.text}")

    # 5. Cleanup: Delete the blob from storage
    orchestrator_connection.log_trace(f"Deleting blob {blob_path} from Azure storage.")
    blob_client.delete_blob(delete_snapshots="include")