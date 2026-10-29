import os
import time
from botocore.exceptions import ClientError
from archive import s3_client

client = s3_client()
bucket = os.environ.get("S3_BUCKET", "cdc-raw")
for attempt in range(60):
    try:
        client.list_buckets()
        break
    except Exception:
        if attempt == 59:
            raise
        time.sleep(2)
try:
    client.head_bucket(Bucket=bucket)
except ClientError as exc:
    if exc.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
        raise
    client.create_bucket(Bucket=bucket)
print(f"Bucket {bucket} is ready.")
