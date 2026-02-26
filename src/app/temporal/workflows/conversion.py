from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class CsvConversionWorkflow:
    @workflow.run
    async def run(self, entry_id: int) -> str:
        ingestion = await workflow.execute_activity(
            "get_ingestion",
            entry_id,
            schedule_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=5,
            ),
        )

        if ingestion is None:
            raise RuntimeError(f"Ingestion {entry_id} does not exist")

        status = ingestion["status"]

        if status == "new":
            await workflow.execute_activity(
                "convert_to_csv_and_mark_converted",
                entry_id,
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=5,
                ),
            )
            ingestion = await workflow.execute_activity(
                "get_ingestion",
                entry_id,
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=5,
                ),
            )
            status = ingestion["status"] if ingestion else None

        if status == "converted":
            await workflow.execute_activity(
                "upload_csv_to_s3_and_mark_uploaded",
                entry_id,
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=5,
                ),
            )
            ingestion = await workflow.execute_activity(
                "get_ingestion",
                entry_id,
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=5,
                ),
            )
            status = ingestion["status"] if ingestion else None

        if status == "uploaded":
            await workflow.execute_activity(
                "process_csv_file",
                ingestion["s3_path"],
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=5,
                ),
            )
            return "uploaded"

        raise RuntimeError(
            f"Ingestion {entry_id} has unsupported or unexpected status '{status}'"
        )