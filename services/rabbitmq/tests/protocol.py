"""Synthetic tests in an isolated vhost, S3 prefix and application database."""
import copy
from datetime import datetime,date,timedelta,timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
import pika
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from admin import api,escaped
from common import Stats,Store,arrow_schema,atomic_json,digest,encoded,load_contract,paths,require
from messaging import connection,database,message_for,publish
from topology import definitions,permissions,QUEUE,DEAD_QUEUE

tag=uuid.uuid4().hex
root=Path('/validation')/tag;root.mkdir(parents=True)
vhost='/validation-invoice-'+tag
prefix='validation/rabbitmq-v1/'+tag
spec=copy.deepcopy(load_contract('invoice_details'))
spec.update(data_prefix=prefix+'/datasets',publication_prefix=prefix+'/published',rows_per_file=2,row_group_rows=1)
atomic_json(root/'contract.json',spec)
env=os.environ.copy()
env.update(RABBIT_VHOST=vhost,INVOICE_CONTRACT_FILE=str(root/'contract.json'),
           APP_STATE_DIR=str(root/'app'),DISPATCH_STATE_DIR=str(root/'dispatcher'),
           MESSAGING_TEST_MODE='1',S3_CREDENTIALS_FILE='/run/secrets/test_reader_s3')
os.environ.update(env)
defs=definitions(vhost);defs['permissions']=permissions(vhost)
require(not any(v['name']==vhost for v in api('GET','vhosts')),'Refusing an existing test vhost')
api('POST','definitions',defs)
writer=Store('/run/secrets/fixture_writer_s3')
checks=[]


def depth(queue=QUEUE):
    with connection('/run/secrets/admin') as conn:
        return conn.channel().queue_declare(queue=queue,passive=True).method.message_count


def wait_depth(count,queue=QUEUE):
    for _ in range(40):
        if depth(queue)==count: return
        time.sleep(.5)
    raise AssertionError('Unexpected queue depth: '+queue)


def child(script,fault='',expected=0,extra_env=None):
    values={**env,'MESSAGING_FAULT':fault,**(extra_env or {})}
    if script=='invoice_app.py':
        values['RABBIT_CREDENTIALS_FILE']='/run/secrets/invoice_app'
    else: values['RABBIT_CREDENTIALS_FILE']='/run/secrets/publisher'
    result=subprocess.run(['python',script]+(['once'] if script=='invoice_app.py' else []),
        env=values,text=True,capture_output=True,timeout=120)
    require(result.returncode==expected,'Child failed: '+result.stdout+result.stderr)
    return result.stdout


def app_state():
    path=root/'app/invoices.sqlite3'
    if not path.exists(): return (0,0,None)
    with database(path) as db:
        active=db.execute('SELECT event_id FROM active_snapshot WHERE id=1').fetchone()
        return (db.execute('SELECT count(*) FROM invoice_lines').fetchone()[0],
                db.execute('SELECT count(*) FROM deliveries').fetchone()[0],active[0] if active else None)


base=datetime.now(timezone.utc)
def fixture(number,ids,corrupt=False):
    run=base.strftime('%Y%m%dT%H%M%SZ-')+uuid.uuid4().hex
    data,publication=paths(spec,run)
    rows=[]
    for i in ids:
        rows.append(dict(invoice_line_id=i,invoice_id=100,order_id=None,customer_id=1,customer_name='Synthetic',
            bill_to_customer_id=1,bill_to_customer_name='Synthetic',invoice_date=date(2026,10,28),
            is_credit_note=False,stock_item_id=1,description='Validation only',quantity=1,unit_price=Decimal('1.00'),
            tax_rate=Decimal('20.000'),tax_amount=Decimal('0.20'),line_profit=Decimal('0.10'),extended_price=Decimal('1.20')))
    table=pa.Table.from_pylist(rows,schema=arrow_schema(spec))
    stats=Stats(spec);stats.add(table);stats=stats.result();files=[]
    for offset in range(0,len(ids),2):
        path=root/'part.parquet';part=table.slice(offset,2);pq.write_table(part,path,compression='zstd')
        key=data+f'/data/part-{len(files)+1:05d}.parquet';content=path.read_bytes()
        writer.upload(key,path)
        files.append(dict(key=key,rows=part.num_rows,bytes=len(content),sha256=digest(content)))
    manifest=dict(protocol='full-export/v1',run_id=run,contract=spec,contract_sha256=digest(encoded(spec)),
        snapshot={'captured_at':(base+timedelta(seconds=number)).isoformat()},statistics=stats,
        source_aggregates={'rows':len(rows),'sums':stats['sums']},files=files)
    marker=dict(protocol='full-export/v1',dataset='invoice_details',version=1,run_id=run,
        manifest_key=data+'/manifest.json',manifest_sha256=digest(encoded(manifest)))
    writer.immutable_json(marker['manifest_key'],manifest);writer.immutable_json(publication,marker)
    if corrupt:
        key=files[-1]['key'];body=writer.get(key)
        writer.client.put_object(Bucket=writer.bucket,Key=key,Body=b'X'+body[1:])
    return message_for(marker,publication,spec)


first=fixture(1,[1,2,3])
child('dispatcher.py','after-outbox',86);wait_depth(0)
checks.append('crash after durable outbox loses no pending notification')
child('dispatcher.py','after-confirm',86);wait_depth(1)
child('dispatcher.py');wait_depth(2)
checks.append('ambiguous publisher success produces a stable duplicate event ID')
child('invoice_app.py','during-replacement',86)
require(app_state()[:2]==(0,0),'Uncommitted rows survived crash');wait_depth(2)
checks.append('crash during local replacement rolls back all application rows')
child('invoice_app.py','after-app-commit',86)
require(app_state()[:2]==(3,1),'Committed application transaction lost');wait_depth(2)
child('invoice_app.py');child('invoice_app.py');wait_depth(0)
require(app_state()[:2]==(3,1),'Duplicate application effect')
checks.append('crash after app commit before broker ACK redelivers without duplicate effect')
second=fixture(2,[1,2]);child('dispatcher.py');child('invoice_app.py');wait_depth(0)
require(app_state()==(2,2,second['event_id']),'Full replacement/deletion failed')
checks.append('new full snapshot atomically removes rows absent from the new delivery')
older=fixture(0,[4,5]);child('dispatcher.py');child('invoice_app.py');wait_depth(0)
require(app_state()==(2,3,second['event_id']),'Older snapshot replaced current state')
checks.append('late older snapshot is validated and recorded without rolling current data backward')
bad=fixture(3,[1,2,3],corrupt=True);child('dispatcher.py');child('invoice_app.py')
wait_depth(0);wait_depth(1,DEAD_QUEUE)
require(app_state()==(2,3,second['event_id']),'Corruption changed current application state')
checks.append('corruption in a later Parquet file rolls back earlier inserts and reaches DLQ')
forged=dict(first,bucket='cdc-raw')
with connection('/run/secrets/publisher') as conn:
    channel=conn.channel();channel.confirm_delivery();publish(channel,forged)
child('invoice_app.py');wait_depth(2,DEAD_QUEUE)
require(app_state()==(2,3,second['event_id']),'Malformed notification changed data')
checks.append('forged bucket/notification is rejected before reading data')
retry=fixture(4,[1]);child('dispatcher.py')
retry_count=0
while depth(DEAD_QUEUE)<3 and retry_count<8:
    child('invoice_app.py','transient');retry_count+=1
    time.sleep(.5)
wait_depth(3,DEAD_QUEUE);wait_depth(0)
checks.append('transient failures stop retrying at the broker delivery limit and reach DLQ')

# A refused route must fail publication, even when the exchange accepted the publish.
with connection('/run/secrets/publisher') as conn:
    channel=conn.channel();channel.confirm_delivery()
    try: publish(channel,first,routing_key='not-bound')
    except pika.exceptions.UnroutableError: checks.append('mandatory unroutable publish is detected')
    else: raise AssertionError('Unroutable publish accepted')

for creds,operation,label in [
    ('publisher',lambda ch:ch.queue_declare(queue='forbidden'), 'publisher cannot declare queues'),
    ('publisher',lambda ch:ch.basic_get(QUEUE),'publisher cannot read application queue'),
    ('invoice_app',lambda ch:ch.queue_declare(queue='forbidden'),'application cannot declare queues'),
    ('invoice_app',lambda ch:ch.basic_get(DEAD_QUEUE),'application cannot consume its DLQ'),
    ('invoice_app',lambda ch:(ch.confirm_delivery(),publish(ch,first)),'application cannot publish')]:
    try:
        with connection('/run/secrets/'+creds) as conn: operation(conn.channel())
    except pika.exceptions.ChannelClosedByBroker as exc:
        require(exc.reply_code==403,'Unexpected AMQP error');checks.append(label)
    else: raise AssertionError(label)

for secret,action,label in [
    ('app_s3',lambda s:s.client.list_objects_v2(Bucket='cdc-raw',MaxKeys=1),'invoice app cannot list CDC raw'),
    ('app_s3',lambda s:s.client.get_object(Bucket=s.bucket,Key='published/dataset=order_details/absent'),'invoice app cannot read order exports'),
    ('app_s3',lambda s:s.client.put_object(Bucket=s.bucket,Key='datasets/dataset=invoice_details/forbidden',Body=b'no'),'invoice app cannot write exports'),
    ('publisher_s3',lambda s:s.client.get_object(Bucket=s.bucket,Key='datasets/dataset=invoice_details/absent'),'dispatcher cannot read Parquet data')]:
    try: action(Store('/run/secrets/'+secret))
    except ClientError as exc:
        require(exc.response['ResponseMetadata']['HTTPStatusCode']==403,'Unexpected S3 access response');checks.append(label)
    else: raise AssertionError(label)
require(not any(u['name']=='guest' for u in api('GET','users')),'Guest user remains enabled')
checks.append('broker has no guest account')

report=dict(checks=checks,retry_attempts_before_dlq=retry_count,synthetic_rows_after_failures=app_state()[0],
    synthetic_deliveries_committed=app_state()[1],test_vhost=vhost,synthetic_s3_prefix=prefix,
    verified_at=datetime.now(timezone.utc).isoformat(),production_data_modified=False)
api('DELETE','vhosts/'+escaped(vhost))
for key in list(writer.list(prefix+'/')):
    require(key.startswith(prefix+'/'),'Unsafe cleanup path')
    writer.client.delete_object(Bucket=writer.bucket,Key=key)
report['test_vhost_and_s3_fixture_removed']=True
atomic_json(Path('/validation/protocol-report.json'),report)
print(json.dumps(report))
