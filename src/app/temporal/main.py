import sys
from pathlib import Path
from os import getenv
from asyncio import run
from temporalio.client import Client
from temporalio.worker import Worker

# Ensure the app root (parent of `temporal/`) is on sys.path so `services` is importable
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from workflows.conversion import CsvConversionWorkflow
from workflows.ingestion import CsvIngestionWorkflow
from temporal.activities import (
    get_ingestion,
    convert_to_csv_and_mark_converted,
    upload_csv_to_s3_and_mark_uploaded,
    process_csv_file,
    ingest_csv_from_s3,
)

async def main() -> None:
    address = getenv("TEMPORAL_ADDRESS", "temporal:7233")
    namespace = getenv("TEMPORAL_NAMESPACE", "default")
    csvTaskQueue = getenv("BG_TASK_QUEUE", "background-task-queue")

    client = await Client.connect(address, namespace=namespace)

    # Run a worker for the same task queue the API uses
    worker = Worker(
        client,
        task_queue=csvTaskQueue,
        workflows=[
            CsvConversionWorkflow,
            CsvIngestionWorkflow,
        ],
        activities=[
            get_ingestion,
            convert_to_csv_and_mark_converted,
            upload_csv_to_s3_and_mark_uploaded,
            process_csv_file,
            ingest_csv_from_s3,
        ],
    )

    print(f"Temporal worker started on '{csvTaskQueue}'")
    await worker.run()


if __name__ == "__main__":
    run(main())

