"""Verify effective read-only permissions without touching business objects."""
import json
import uuid
from archive import s3_client
from load import connect
from botocore.exceptions import ClientError
import psycopg


def main():
    with connect() as conn:
        assert conn.execute('SHOW default_transaction_read_only').fetchone()[0]=='on'
        assert conn.execute("SELECT has_table_privilege(current_user,'staging.cdc_event','INSERT'),has_table_privilege(current_user,'ops.consumer_batches','UPDATE')").fetchone()==(False,False)
        conn.execute('SELECT event_id,semantic_sha256 FROM staging.cdc_event LIMIT 1')
        try: conn.execute('SELECT row_data FROM staging.cdc_event LIMIT 1')
        except psycopg.errors.InsufficientPrivilege: pass
        else: raise AssertionError('Audit role can read unneeded business row contents')
    client=s3_client()
    key='validation/purge-audit-v1/denied-'+uuid.uuid4().hex+'.txt'
    try: client.put_object(Bucket='cdc-raw',Key=key,Body=b'audit access probe')
    except ClientError as error:
        assert error.response['Error']['Code']=='AccessDenied'
    else: raise AssertionError('Audit S3 identity unexpectedly wrote isolated probe: '+key)
    try: client.list_objects_v2(Bucket='warehouse-exports',MaxKeys=1)
    except ClientError as error:
        assert error.response['Error']['Code']=='AccessDenied'
    else: raise AssertionError('Audit S3 identity can list exports outside its scope')
    print(json.dumps({'warehouse_read_only':True,'business_row_access_denied':True,
                      's3_write_denied':True,'export_bucket_list_denied':True}))


if __name__=='__main__': main()
