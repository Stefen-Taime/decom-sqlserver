"""Verify the consumer's S3 identity can list/read but cannot write archive objects."""
import json
import uuid
from botocore.exceptions import ClientError
from archive import s3_client

client=s3_client()
objects=client.list_objects_v2(Bucket='cdc-raw',Prefix='raw/wwi/bootstrap-v2/manifests/',MaxKeys=1)
key=objects['Contents'][0]['Key']
response=client.get_object(Bucket='cdc-raw',Key=key)
response['Body'].close()
try:
    client.put_object(Bucket='cdc-raw',Key='_permission-checks/warehouse-'+str(uuid.uuid4()),Body=b'permission-test')
except ClientError as error:
    if error.response['ResponseMetadata']['HTTPStatusCode']!=403:
        raise
else:
    raise RuntimeError('Archive reader unexpectedly has write permission; review identity before proceeding')
print(json.dumps({'archive_list':'passed','archive_read':'passed','archive_write_denied':True}))
