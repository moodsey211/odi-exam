from os import getenv
from typing import Optional
import hashlib

from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

temporal_address = getenv("TEMPORAL_ADDRESS", "temporal:7233")
temporal_namespace = getenv("TEMPORAL_NAMESPACE", "default")
bg_task_queue = getenv("BG_TASK_QUEUE", "background-task-queue")

_client: Optional[Client] = None

async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(
            temporal_address,
            namespace=temporal_namespace,
        )
    return _client

async def process_csv_file(s3path: str):
    client = await get_temporal_client()
    
    # Calculate MD5 hash of s3path to use as workflow_id
    workflow_id = hashlib.md5(s3path.encode()).hexdigest()
    
    try:
        # Check if workflow already exists
        handle = client.get_workflow_handle(workflow_id)
        workflow_status = await handle.describe()
        status = workflow_status.status.name.lower()
        
        # If status is completed, running, or terminated, do nothing
        if status in ["completed", "running", "terminated"]:
            return
        
        # If status is failed, retry the workflow
        if status == "failed":
            await handle.retry()
            return
    
    except RPCError as e:
        if e.status != RPCStatusCode.NOT_FOUND:
            raise
        # Workflow doesn't exist, start it
        pass
    
    # Start the workflow if it doesn't exist
    await client.start_workflow(
        "CsvIngestionWorkflow",
        s3path,
        id=workflow_id,
        task_queue=bg_task_queue,
    )

async def start_csv_conversion(entry_id: int):
    client = await get_temporal_client()
    workflow_id = f"ingest-{entry_id}"

    await client.start_workflow(
        "CsvConversionWorkflow",
        entry_id,
        id=workflow_id,
        task_queue=bg_task_queue,
    )