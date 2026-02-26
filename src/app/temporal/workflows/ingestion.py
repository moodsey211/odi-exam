from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class CsvIngestionWorkflow:
    @workflow.run
    async def run(self, s3path: str) -> str:
        result = await workflow.execute_activity(
            "ingest_csv_from_s3",
            s3path,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return result
