"""Real PostgreSQL tests: raw preservation, proof boundaries, replay and business reads."""
from copy import deepcopy
import os
from pathlib import Path
import psycopg
from psycopg.types.json import Jsonb
import pytest
from test_pipeline import conn,archive,event,batch,unique_id
from load import apply_batch,transform,digest,connect
from resolve import rebuild_keys
from resolve_core import MARKER


def order(key,op='c',seq=1,value='known',before=None):
    item=event(key,op,seq,order=True,value=value)
    if before is not None: item['payload']['before']['Comments']=before
    return item


def identity(item): return transform(item['payload'],'0'*64)[0]


def admin():
    return psycopg.connect(host=os.environ.get('PGHOST','postgres'),dbname='warehouse_test',
        user='warehouse_admin',password=Path('/run/secrets/test_admin').read_text().strip(),autocommit=True)


def reader():
    return psycopg.connect(host=os.environ.get('PGHOST','postgres'),dbname='warehouse_test',
        user='warehouse_reader',password=Path('/run/secrets/reader').read_text().strip(),autocommit=True)


def attest(conn,item,images=('before','after')):
    row=transform(item['payload'],'0'*64)
    with admin() as owner:
        for image in images:
            if item['payload'][image].get('Comments')!=MARKER: continue
            owner.execute('''INSERT INTO business.marker_proof(event_id,image,column_name,semantic_sha256,
                proof_sha256,source_observation_sha256,evidence) VALUES(%s,%s,'Comments',%s,%s,%s,%s)
                ON CONFLICT DO NOTHING''',(row[0],image,row[1],digest([row[0],image]),'a'*64,
                                         Jsonb({'fixture':'isolated SQL proof-boundary test'})))
    with conn.transaction(): rebuild_keys(conn,[('Sales.Orders',item['payload']['after']['OrderID'])])


def current(conn,key):
    with reader() as read:
        return read.execute('SELECT attributes FROM mart.orders WHERE order_id=%s',(key,)).fetchone()


def result(conn,item):
    return conn.execute('SELECT status,resolved_before,resolved_after,lineage,problems,revision_sha256,resolved_at '
                        'FROM business.event_resolution WHERE event_id=%s',(identity(item),)).fetchone()


def test_marker_needs_proof_and_does_not_silently_filter_rows(conn,archive):
    key=unique_id();start=order(key);update=order(key,'u',2,MARKER)
    apply_batch(conn,*batch(archive,[start,update]))
    assert result(conn,update)[0]=='blocked'
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): current(conn,key)
    # Even a read not selecting the affected column must not quietly publish the row.
    with reader() as read,pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        read.execute('SELECT order_id FROM mart.orders WHERE order_id=%s',(key,)).fetchall()
    with reader() as read,pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        read.execute('SELECT row_to_json(c) FROM delivery.ingestion_context c').fetchall()
    attest(conn,update)
    assert current(conn,key)[0]['Comments']=='known'
    assert result(conn,update)[1]['Comments']=='known'
    with reader() as read:
        assert len(read.execute('SELECT row_to_json(c) FROM delivery.ingestion_context c').fetchall())==1
    assert conn.execute('SELECT row_data->>\'Comments\' FROM staging.cdc_event WHERE event_id=%s',
                        (identity(update),)).fetchone()[0]==MARKER
    assert conn.execute('SELECT attributes->>\'Comments\' FROM dv.s_order WHERE event_id=%s',
                        (identity(update),)).fetchone()[0]==MARKER


def test_known_null_can_be_reconstructed_but_missing_is_unknown(conn,archive):
    key=unique_id();start=order(key,value=None);update=order(key,'u',2,MARKER)
    apply_batch(conn,*batch(archive,[start,update]));attest(conn,update)
    assert current(conn,key)[0]['Comments'] is None
    missing_key=unique_id();start=order(missing_key);del start['payload']['after']['Comments']
    update=order(missing_key,'u',2,MARKER)
    apply_batch(conn,*batch(archive,[start,update]));attest(conn,update)
    assert result(conn,update)[0]=='blocked'


def test_explicit_null_update_replaces_a_known_string(conn,archive):
    key=unique_id();start=order(key,value='known');clear=order(key,'u',2,None,before='known')
    update=order(key,'u',3,MARKER)
    apply_batch(conn,*batch(archive,[start,clear,update]));attest(conn,update)
    assert current(conn,key)[0]['Comments'] is None
    assert result(conn,update)[3]['after.Comments']['value_event_id']==identity(clear)


def test_late_earlier_event_rebuilds_marker_dependency(conn,archive):
    key=unique_id();start=order(key,value='initial');update=order(key,'u',4,MARKER)
    apply_batch(conn,*batch(archive,[update,start]));attest(conn,update)
    first=result(conn,update)
    assert current(conn,key)[0]['Comments']=='initial'
    late=order(key,'u',3,'newer-known',before='initial')
    apply_batch(conn,*batch(archive,[late]))
    assert current(conn,key)[0]['Comments']=='newer-known'
    assert result(conn,update)[5]!=first[5]
    assert result(conn,update)[3]['after.Comments']['value_event_id']==identity(late)


def test_missing_seed_and_late_seed(conn,archive):
    key=unique_id();update=order(key,'u',2,MARKER)
    apply_batch(conn,*batch(archive,[update]));attest(conn,update)
    assert result(conn,update)[0]=='blocked'
    apply_batch(conn,*batch(archive,[order(key)]))
    assert current(conn,key)[0]['Comments']=='known'


def test_delete_is_a_barrier_and_recreate_starts_new_history(conn,archive):
    key=unique_id();bad=order(key,'u',3,MARKER)
    apply_batch(conn,*batch(archive,[order(key),order(key,'d',2),bad]));attest(conn,bad)
    assert result(conn,bad)[0]=='blocked'
    good=order(key,'u',5,MARKER)
    apply_batch(conn,*batch(archive,[order(key,'c',4,'new-lifecycle'),good]));attest(conn,good)
    assert current(conn,key)[0]['Comments']=='new-lifecycle'
    apply_batch(conn,*batch(archive,[order(key,'d',6,'new-lifecycle')]))
    assert current(conn,key) is None


def test_duplicate_and_rebatched_replay_do_not_change_revision(conn,archive):
    key=unique_id();update=order(key,'u',2,MARKER)
    args=batch(archive,[order(key),update]);apply_batch(conn,*args);attest(conn,update)
    initial=result(conn,update)
    assert apply_batch(conn,*args)==0
    copy=deepcopy(update);copy['payload']['ts_ms']+=86400000
    assert apply_batch(conn,*batch(archive,[copy]))==0
    assert result(conn,update)==initial


def test_resolution_and_ack_rollback_together(conn,archive):
    key=unique_id();update=order(key,'u',2,MARKER)
    args=batch(archive,[order(key),update])
    with pytest.raises(RuntimeError): apply_batch(conn,*args,fail_after_stage=True)
    assert result(conn,update) is None
    assert conn.execute('SELECT count(*) FROM business.resolution_input WHERE event_id=%s',
                        (identity(update),)).fetchone()[0]==0
    assert apply_batch(conn,*args)==2


def test_marker_chains_keep_original_value_provenance(conn,archive):
    key=unique_id();start=order(key);first=order(key,'u',2,MARKER);second=order(key,'u',3,MARKER)
    apply_batch(conn,*batch(archive,[second,first,start]));attest(conn,first);attest(conn,second)
    assert current(conn,key)[0]['Comments']=='known'
    assert result(conn,second)[3]['after.Comments']['value_event_id']==identity(start)


def test_unproved_marker_cannot_supply_a_later_marker(conn,archive):
    key=unique_id();first=order(key,'u',2,MARKER);second=order(key,'u',3,MARKER)
    apply_batch(conn,*batch(archive,[order(key),first,second]));attest(conn,second)
    assert result(conn,second)[0]=='blocked'


def test_literal_marker_and_unexpected_column_fail_closed(conn,archive):
    key=unique_id();literal=order(key,value=MARKER)
    apply_batch(conn,*batch(archive,[literal]))
    assert result(conn,literal)[0]=='blocked'
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState): current(conn,key)
    key=unique_id();update=order(key,'u',2)
    update['payload']['after']['CustomerPurchaseOrderNumber']=MARKER
    apply_batch(conn,*batch(archive,[order(key),update]))
    assert result(conn,update)[0]=='blocked'


def test_loader_cannot_certify_its_own_source_proofs(conn):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute('INSERT INTO business.marker_proof SELECT * FROM business.marker_proof')
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute('DELETE FROM business.resolution_input')
