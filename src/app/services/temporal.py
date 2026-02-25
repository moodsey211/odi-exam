from os import getenv
from typing import Optional

from temporalio.client import Client

temporal_address = getenv("TEMPORAL_ADDRESS", "temporal:7233")
temporal_namespace = getenv("TEMPORAL_NAMESPACE", "default")
csv_task_queue = getenv("CSV_TASK_QUEUE", "csv-conversion-queue")

_client: Optional[Client] = None


async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(
            temporal_address,
            namespace=temporal_namespace,
        )
    return _client


async def start_csv_conversion(entry_id: int):
    client = await get_temporal_client()
    workflow_id = f"ingest-{entry_id}"

    await client.start_workflow(
        "CsvConversionWorkflow",
        entry_id,
        id=workflow_id,
        task_queue=csv_task_queue,
    )