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
from services.database import get_db
from temporal.activities import (  # noqa: E402
    get_ingestion,
    convert_to_csv_and_mark_converted,
    upload_csv_to_s3_and_mark_uploaded,
)

async def main() -> None:
    address = getenv("TEMPORAL_ADDRESS", "temporal:7233")
    namespace = getenv("TEMPORAL_NAMESPACE", "default")
    csvTaskQueue = getenv("CSV_TASK_QUEUE", "csv-conversion-queue")

    client = await Client.connect(address, namespace=namespace)

    # Run a worker for the same task queue the API uses
    worker = Worker(
        client,
        task_queue=csvTaskQueue,
        workflows=[CsvConversionWorkflow],
        activities=[
            get_ingestion,
            convert_to_csv_and_mark_converted,
            upload_csv_to_s3_and_mark_uploaded,
        ],
    )

    print(f"Temporal worker started on '{csvTaskQueue}'")
    await worker.run()


if __name__ == "__main__":
    run(main())

