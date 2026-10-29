"""Run with the actual consumer identity and only its network/secrets."""
import json
import os
from botocore.exceptions import ClientError
from common import Store, require

store=Store()
checks=[]
for label, callback in [
    ('consumer write denied',lambda:store.client.put_object(Bucket=store.bucket,Key='validation/forbidden-consumer-write',Body=b'no')),
    ('consumer delete denied',lambda:store.client.delete_object(Bucket=store.bucket,Key='validation/absent-object')),
    ('consumer raw list denied',lambda:store.client.list_objects_v2(Bucket='cdc-raw',MaxKeys=1)),
    ('consumer raw read denied',lambda:store.client.get_object(Bucket='cdc-raw',Key='raw/wwi/bootstrap-v2/absent'))]:
    try:
        callback()
    except ClientError as exc:
        require(exc.response['ResponseMetadata']['HTTPStatusCode']==403,'Unexpected access response')
        checks.append(label)
    else:
        raise AssertionError(label)
require('PGPASSWORD_FILE' not in os.environ and 'PGHOST' not in os.environ,'Consumer received database configuration')
checks.append('consumer receives no PostgreSQL configuration or credential')
print(json.dumps({'checks':checks}))
