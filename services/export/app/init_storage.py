from botocore.exceptions import ClientError
from common import Store
store = Store()
try:
    store.client.head_bucket(Bucket=store.bucket)
except ClientError as exc:
    if exc.response['ResponseMetadata']['HTTPStatusCode'] != 404:
        raise
    store.client.create_bucket(Bucket=store.bucket)
print('Export bucket is ready.')
