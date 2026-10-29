"""Integration tests against an isolated PostgreSQL database, never the warehouse."""
from copy import deepcopy
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import uuid

import psycopg
import pytest
from botocore.exceptions import ClientError

sys.path.insert(0,'/app')
from archive import ArchiveStore
from load import apply_batch, connect, verified_batch


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def get_object(self,Key,**kwargs):
        if Key not in self.objects:
            raise ClientError({'Error':{'Code':'NoSuchKey'}},'GetObject')
        return {'Body':BytesIO(self.objects[Key])}

    def put_object(self,Key,Body,**kwargs):
        self.objects[Key] = Body


@pytest.fixture
def conn():
    assert os.environ['PGDATABASE']=='warehouse_test', 'Tests require the isolated database'
    with connect() as connection:
        yield connection


@pytest.fixture
def archive():
    return ArchiveStore(MemoryS3(),'cdc-raw')


def event(key, op='c', sequence=1, *, order=False, customer=1, value='before'):
    table,pk = ('Orders','OrderID') if order else ('Customers','CustomerID')
    row = {pk:key,'CustomerID':customer,'Comments':value,'OrderDate':'2026-10-28','ExpectedDeliveryDate':'2026-10-29'} if order else {
        pk:key,'CustomerName':value,'BillToCustomerID':key,'CreditLimit':'100.00','IsOnCreditHold':False}
    source = {'db':'WideWorldImporters','schema':'Sales','table':table,'snapshot': 'true' if op=='r' else 'false',
        'commit_lsn':f'00000001:00000001:{sequence:04x}', 'change_lsn':f'00000001:00000001:{sequence:04x}', 'event_serial_no':2 if op=='u' else 1,'ts_ms':1793203200000}
    if op=='r':
        source['commit_lsn']=os.environ['SNAPSHOT_COMMIT_LSN']
    return {'payload':{'source':source,'ts_ms':1793203200000,'op':op,
        'before':deepcopy(row) if op in {'u','d'} else None,'after':None if op=='d' else row}}


def batch(archive, events):
    result=archive.archive(json.dumps(events).encode())
    receipt, checksum, values=verified_batch(archive,result['receipt_key'])
    return receipt,result['receipt_key'],checksum,values


def unique_id():
    return int(uuid.uuid4().hex[:12],16)


def test_atomic_rollback_and_retry(conn,archive):
    key=unique_id()
    args=batch(archive,[event(key)])
    with pytest.raises(RuntimeError):
        apply_batch(conn,*args,fail_after_stage=True)
    assert conn.execute('SELECT count(*) FROM staging.cdc_event WHERE source_pk=%s',(key,)).fetchone()[0]==0
    assert conn.execute('SELECT count(*) FROM dv.h_customer WHERE customer_id=%s',(key,)).fetchone()[0]==0
    assert conn.execute('SELECT count(*) FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()[0]==0
    assert apply_batch(conn,*args)==1
    assert apply_batch(conn,*args)==0


def test_event_rebatched_and_capture_clock_changed_is_deduplicated(conn,archive):
    item=event(unique_id())
    assert apply_batch(conn,*batch(archive,[item]))==1
    item['payload']['ts_ms']+=86400000
    assert apply_batch(conn,*batch(archive,[item]))==0


def test_conflicting_replay_rolls_back_without_checkpoint(conn,archive):
    key=unique_id()
    apply_batch(conn,*batch(archive,[event(key)]))
    args=batch(archive,[event(key,value='conflicting')])
    with pytest.raises(ValueError,match='different source content'):
        apply_batch(conn,*args)
    assert conn.execute('SELECT count(*) FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()[0]==0


def test_delete_late_updates_and_snapshot_never_resurrect_current_row(conn,archive):
    key=unique_id()
    for op,seq,value in [('d',5,'deleted'),('u',4,'older'),('c',2,'created'),('r',1,'snapshot')]:
        apply_batch(conn,*batch(archive,[event(key,op,seq,value=value)]))
    assert conn.execute('SELECT count(*) FROM staging.current_key k JOIN dv.s_customer s USING(event_id) WHERE k.source_pk=%s AND s.is_deleted',(key,)).fetchone()[0]==1
    assert conn.execute('SELECT count(*) FROM dv.s_customer s JOIN dv.h_customer h USING(customer_hk) WHERE h.customer_id=%s',(key,)).fetchone()[0]==4
    apply_batch(conn,*batch(archive,[event(key,'c',6,value='recreated')]))
    assert conn.execute('SELECT s.attributes->>\'CustomerName\' FROM staging.current_key k JOIN dv.s_customer s USING(event_id) WHERE k.source_pk=%s',(key,)).fetchone()[0]=='recreated'


def test_order_relation_changes_keep_history_and_latest_customer(conn,archive):
    key,customer_a,customer_b=unique_id(),unique_id(),unique_id()
    apply_batch(conn,*batch(archive,[event(key,'u',4,order=True,customer=customer_b),event(key,'c',2,order=True,customer=customer_a)]))
    assert conn.execute('SELECT count(*) FROM dv.l_order_customer l JOIN dv.h_order h USING(order_hk) WHERE h.order_id=%s',(key,)).fetchone()[0]==2
    current=conn.execute('SELECT h.customer_id FROM staging.current_key k JOIN dv.s_order s USING(event_id) JOIN dv.l_order_customer l USING(order_customer_hk) JOIN dv.h_customer h USING(customer_hk) WHERE k.source_pk=%s',(key,)).fetchone()[0]
    assert current==customer_b


def test_corruption_and_bad_manifest_counts_rejected(archive):
    args=batch(archive,[event(unique_id())])
    data_key=args[0]['payload_key']
    original=archive.client.objects[data_key]
    archive.client.objects[data_key]=b'corrupt'
    with pytest.raises(IOError):
        verified_batch(archive,args[1])
    archive.client.objects[data_key]=original
    manifest=json.loads(archive.client.objects[args[1]])
    manifest['records']+=1
    archive.client.objects[args[1]]=json.dumps(manifest).encode()
    with pytest.raises(ValueError):
        verified_batch(archive,args[1])


def test_mixed_batch_keeps_only_pilot_events_and_counts_ignored(conn,archive):
    item=event(unique_id())
    other={'payload':{'source':{'db':'WideWorldImporters','schema':'lab','table':'CdcProbe','commit_lsn':'00000001:00000001:0001'},'ts_ms':1793203200000,'op':'c','after':{'ProbeID':'scope-test'}}}
    args=batch(archive,[item,other])
    assert '/table=_mixed/' in args[1]
    assert apply_batch(conn,*args)==1
    assert conn.execute('SELECT total_records,selected_records,ignored_records FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()==(2,1,1)


def test_loader_cannot_delete_or_create_permanent_tables(conn):
    for query in ['DELETE FROM dv.h_customer','CREATE TABLE dv.forbidden(id int)']:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(query)


@pytest.mark.parametrize('marker',['first','first_in_data_collection','true','last_in_data_collection','last'])
def test_initial_snapshot_boundary_markers(conn,archive,marker):
    item=event(unique_id(),'r')
    item['payload']['source']['snapshot']=marker
    assert apply_batch(conn,*batch(archive,[item]))==1


def test_incremental_snapshot_rejected_without_checkpoint(conn,archive):
    item=event(unique_id(),'r')
    item['payload']['source']['snapshot']='incremental'
    args=batch(archive,[item])
    with pytest.raises(ValueError,match='initial snapshot'):
        apply_batch(conn,*args)
    assert conn.execute('SELECT count(*) FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()[0]==0


def test_unexpected_new_snapshot_generation_rejected(conn,archive):
    item=event(unique_id(),'r')
    item['payload']['source']['commit_lsn']='ffffffff:ffffffff:ffff'
    with pytest.raises(ValueError,match='new generation'):
        apply_batch(conn,*batch(archive,[item]))
