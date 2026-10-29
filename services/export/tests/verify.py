"""Live protocol checks; only test-prefix objects are mutated/deleted."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
import psycopg
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from common import (Stats, Store, arrow_schema, atomic_json, digest, encoded, load_contract,
                    paths, require, verify_manifest)
from export import connect
from tests.test_contract import row
import pyarrow as pa


def launch(dataset, request, fault=None):
    env = os.environ.copy()
    env.update(EXPORT_TEST_MODE='1', EXPORT_FAULT=fault or '')
    result = subprocess.run(['python','export.py','--dataset',dataset,'--request-id',request],
        env=env,text=True,capture_output=True)
    require(result.returncode == (86 if fault else 0), f'Unexpected export result: {result.stdout} {result.stderr}')
    print(result.stdout, end='', flush=True)
    if not fault:
        return json.loads(result.stdout.strip().splitlines()[-1])


def request_state(dataset, request):
    return json.loads((Path(os.environ['EXPORT_STATE_DIR'])/f'{dataset}-v1-{request}.json').read_text())


def expect_rejected(callback, message):
    try:
        callback()
    except (ValueError,ClientError):
        return
    raise AssertionError(message)


store = Store()
report = {'checks':[], 'exports':{}, 'purge_authorized':False}
tag = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')

# Fail before the commit marker, then after the commit marker but before the local acknowledgement.
request = 'invoice-'+tag
launch('invoice_details', request, 'after-ready')
state = request_state('invoice_details',request)
attempt = state['attempts'][-1]
require(attempt['status']=='ready' and store.get(attempt['publication'],optional=True) is None,'Premature publication')
report['checks'].append('validated manifest without commit marker is undiscoverable')
launch('invoice_details',request,'after-publish')
require(store.get(attempt['publication']) is not None,'Missing ambiguous-success marker')
invoice = launch('invoice_details',request)
require(invoice['attempts']==1 and invoice['run_id']==attempt['run_id'],'Ready snapshot was not reused')
report['checks'].append('crash after publication recovered without duplicate delivery')
report['exports']['invoice_details']=invoice

# Kill while extracting. A resumed DB transaction cannot recreate the old snapshot: new attempt path.
request = 'orders-'+tag
launch('order_details',request,'after-first-file')
first = request_state('order_details',request)['attempts'][0]
contract = load_contract('order_details')
root, commit = paths(contract, first['run_id'])
require(len(list(store.list(root+'/data/')))==1 and store.get(commit,optional=True) is None,'Partial run advertised')
order = launch('order_details',request)
require(order['attempts']==2 and order['run_id']!=first['run_id'],'Incomplete attempt reused')
state = request_state('order_details',request)
require(state['attempts'][0]['status']=='abandoned','Abandoned attempt not audited')
before = store.client.head_object(Bucket=store.bucket,Key=order['publication'])['LastModified']
repeated = launch('order_details',request)
after = store.client.head_object(Bucket=store.bucket,Key=order['publication'])['LastModified']
require(order==repeated and before==after,'Idempotent request rewrote its publication')
report['checks'].extend(['partial run invisible; retry uses fresh snapshot and path','duplicate request returns same immutable publication'])
report['exports']['order_details']=order

# S3 damage tests use an isolated synthetic prefix, never the business deliveries.
contract = copy.deepcopy(load_contract('invoice_details'))
validation_prefix = 'validation/protocol-'+tag+'-'+uuid.uuid4().hex
contract.update(data_prefix=validation_prefix+'/datasets', publication_prefix=validation_prefix+'/published')
run_id = tag+'-'+uuid.uuid4().hex
root, publication = paths(contract,run_id)
with tempfile.TemporaryDirectory() as directory:
    path=Path(directory)/'test.parquet'
    table=pa.Table.from_pylist([row()],schema=arrow_schema(contract))
    pq.write_table(table,path,compression='zstd')
    stats=Stats(contract); stats.add(table); stats=stats.result()
    key=root+'/data/part-00001.parquet'
    original=path.read_bytes()
    store.upload(key,path)
    manifest=dict(protocol='full-export/v1',run_id=run_id,contract=contract,
        contract_sha256=digest(encoded(contract)),statistics=stats,
        source_aggregates={'rows':1,'sums':stats['sums']},
        files=[dict(key=key,rows=1,bytes=len(original),sha256=digest(original))])
    marker=dict(protocol='full-export/v1',dataset=contract['dataset'],version=1,run_id=run_id,
        manifest_key=root+'/manifest.json',manifest_sha256=digest(encoded(manifest)))
    store.immutable_json(marker['manifest_key'],manifest)
    store.immutable_json(publication,marker)
    verify_manifest(store,contract,marker,publication)
    store.client.put_object(Bucket=store.bucket,Key=key,Body=b'X'+original[1:])
    expect_rejected(lambda:verify_manifest(store,contract,marker,publication),'Corrupt data accepted')
    store.client.delete_object(Bucket=store.bucket,Key=key)
    expect_rejected(lambda:verify_manifest(store,contract,marker,publication),'Missing file accepted')
    store.client.put_object(Bucket=store.bucket,Key=key,Body=original)
    store.client.put_object(Bucket=store.bucket,Key=marker['manifest_key'],Body=b'{}')
    expect_rejected(lambda:verify_manifest(store,contract,marker,publication),'Corrupt manifest accepted')
    for object_key in list(store.list(validation_prefix+'/')):
        store.client.delete_object(Bucket=store.bucket,Key=object_key)
report['checks'].extend(['S3 Parquet corruption rejected','S3 missing data file rejected','S3 manifest corruption rejected'])

with connect() as db:
    require(db.execute('SHOW default_transaction_read_only').fetchone()[0]=='on','Exporter defaults to writes')
    privileges=db.execute("SELECT has_table_privilege(current_user,'delivery.invoice_details_v1',%s)",('INSERT,UPDATE,DELETE',)).fetchone()[0]
    require(not privileges,'Exporter has delivery-view write privileges')
    report['checks'].append('exporter defaults to read-only transactions')
for statement in ['SELECT * FROM staging.cdc_event LIMIT 1','SELECT * FROM mart.customers LIMIT 1',
                  'CREATE TABLE delivery.forbidden_probe(id integer)', 'DELETE FROM staging.cdc_event WHERE false']:
    try:
        with connect() as db:
            db.execute('SET TRANSACTION READ WRITE')
            db.execute(statement)
            db.rollback()
    except psycopg.errors.InsufficientPrivilege:
        pass
    else:
        raise AssertionError('Unexpected database privilege')
report['checks'].append('exporter denied raw schemas, DDL and DML even with read-only default disabled')
try:
    store.client.list_objects_v2(Bucket='cdc-raw',MaxKeys=1)
except ClientError as exc:
    require(exc.response['ResponseMetadata']['HTTPStatusCode']==403,'Unexpected raw access response')
else:
    raise AssertionError('Exporter can access raw archive')
report['checks'].append('exporter cannot list raw CDC bucket')
atomic_json(Path(os.environ['EXPORT_STATE_DIR'])/'validation-report.json',report)
print(json.dumps(report))
