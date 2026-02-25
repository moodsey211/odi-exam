from os import getenv
from boto3 import client

S3_BUCKET = getenv("AWS_S3_BUCKET", "csv-uploads")
S3_ENDPOINT_URL = getenv("AWS_S3_ENDPOINT_URL", "http://localstack:4566")
AWS_REGION = getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY", "test")

def upload_csv(filepath: str, filename: str) -> str:
    s3_client = client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_key = f"ingestions/{filename}"
    s3_client.upload_file(str(filepath), S3_BUCKET, s3_key)

    return f"s3://{S3_BUCKET}/{s3_key}"