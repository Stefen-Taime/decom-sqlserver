"""Scope upgrades and the three additional sales entities against real PostgreSQL."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import psycopg
import pytest
from test_pipeline import conn, archive, batch, event, unique_id
import load


@pytest.fixture
def reader():
    with psycopg.connect(host='postgres',dbname='warehouse_test',user='warehouse_reader',
            password=Path('/run/secrets/reader').read_text().strip(),autocommit=True) as connection:
        yield connection


def sales_event(table,key,op='c',seq=1,**fields):
    item=event(key,op,seq)
    item['payload']['source']['table']=table
    pk=load.TABLES['Sales.'+table]
    row={pk:key,**fields}
    item['payload']['before']=deepcopy(row) if op in {'u','d'} else None
    item['payload']['after']=None if op=='d' else row
    return item


def test_v1_mixed_batch_is_revisited_by_v2_without_rewriting_old_ack(conn,archive,monkeypatch):
    key,parent=unique_id(),unique_id()
    args=batch(archive,[event(parent),sales_event('OrderLines',key,OrderID=parent,Quantity=2)])
    with monkeypatch.context() as patch:
        patch.setattr(load,'CONSUMER_VERSION','customers-orders/v1')
        patch.setattr(load,'TABLES',{'Sales.Customers':'CustomerID','Sales.Orders':'OrderID'})
        assert load.apply_batch(conn,*args)==1
    before=conn.execute('SELECT * FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()
    assert load.apply_batch(conn,*args)==1
    assert load.apply_batch(conn,*args)==0
    assert conn.execute('SELECT * FROM ops.archive_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()==before
    assert conn.execute('SELECT selected_records,new_events,ignored_records FROM ops.consumer_batches WHERE consumer_version=%s AND batch_id=%s',
        ('sales/v2',args[0]['batch_id'])).fetchone()==(2,1,0)


@pytest.mark.parametrize('table,parentfield,view,idcol,sat',[
    ('OrderLines','OrderID','order_lines','order_line_id','s_order_line'),
    ('InvoiceLines','InvoiceID','invoice_lines','invoice_line_id','s_invoice_line')])
def test_child_first_parent_change_delete_and_late_events(conn,reader,archive,table,parentfield,view,idcol,sat):
    key,a,b=unique_id(),unique_id(),unique_id()
    # Parents have no satellites yet. Inferred hubs must not cause lost children.
    for op,seq,parent in [('u',3,b),('c',1,a)]:
        load.apply_batch(conn,*batch(archive,[sales_event(table,key,op,seq,**{parentfield:parent,'Quantity':1})]))
    parentcol='order_id' if table=='OrderLines' else 'invoice_id'
    assert reader.execute(f'SELECT {parentcol} FROM mart.{view} WHERE {idcol}=%s',(key,)).fetchone()==(b,)
    load.apply_batch(conn,*batch(archive,[sales_event(table,key,'d',4,**{parentfield:b})]))
    load.apply_batch(conn,*batch(archive,[sales_event(table,key,'u',2,**{parentfield:a})]))
    assert reader.execute(f'SELECT count(*) FROM mart.{view} WHERE {idcol}=%s',(key,)).fetchone()==(0,)
    assert conn.execute(f'SELECT count(*) FROM dv.{sat} s JOIN staging.cdc_event e USING(event_id) WHERE e.source_pk=%s',(key,)).fetchone()==(4,)


def test_invoice_optional_order_roles_computed_fields_and_line_grain(conn,reader,archive):
    inv,cust,bill,order=unique_id(),unique_id(),unique_id(),unique_id()
    fields={'CustomerID':cust,'BillToCustomerID':bill,'OrderID':None,'InvoiceDate':'2026-10-28Z','IsCreditNote':True,
        'ReturnedDeliveryData':json.dumps({'DeliveredWhen':'2026-10-28T12:30:15.1234567','ReceivedBy':'receiver'}),
        'ConfirmedDeliveryTime':None,'ConfirmedReceivedBy':None}
    load.apply_batch(conn,*batch(archive,[sales_event('Invoices',inv,**fields)]))
    assert reader.execute('SELECT customer_id,bill_to_customer_id,order_id,confirmed_delivery_time,confirmed_received_by FROM mart.invoices WHERE invoice_id=%s',
        (inv,)).fetchone()==(cust,bill,None,'2026-10-28T12:30:15.1234567','receiver')
    fields['OrderID']=order
    load.apply_batch(conn,*batch(archive,[sales_event('Invoices',inv,'u',2,**fields)]))
    assert reader.execute('SELECT order_id FROM mart.invoices WHERE invoice_id=%s',(inv,)).fetchone()==(order,)
    fields['OrderID']=None
    load.apply_batch(conn,*batch(archive,[sales_event('Invoices',inv,'u',3,**fields)]))
    rows=[sales_event('InvoiceLines',unique_id(),InvoiceID=inv,Quantity=-2,UnitPrice='0.10',TaxRate='7.500',TaxAmount='-0.03',LineProfit='-0.01',ExtendedPrice='-0.23') for _ in range(2)]
    load.apply_batch(conn,*batch(archive,rows))
    assert reader.execute('SELECT line_count,quantity,tax_amount,line_profit,extended_price,is_credit_note,order_id FROM mart.invoice_summary WHERE invoice_id=%s',
        (inv,)).fetchone()==(2,-4,Decimal('-0.06'),Decimal('-0.02'),Decimal('-0.46'),True,None)
    assert reader.execute('SELECT count(*),count(DISTINCT invoice_line_id) FROM mart.invoice_details WHERE invoice_id=%s',(inv,)).fetchone()==(2,2)
    load.apply_batch(conn,*batch(archive,[sales_event('Invoices',inv,'d',4,**fields)]))
    assert reader.execute('SELECT count(*) FROM mart.invoices WHERE invoice_id=%s',(inv,)).fetchone()==(0,)


@pytest.mark.parametrize('value,expected',[
    ('2026-10-28T12:30:15.1234567Z','2026-10-28T12:30:15.1234567'),
    ('2024-02-29','2024-02-29T00:00:00'),('2025-02-29',None),
    ('2026-10-28T25:00:00',None),('not-a-date',None),(None,None)])
def test_computed_date_contract(reader,value,expected):
    assert reader.execute('SELECT mart.try_iso_datetime2_7(%s)',(value,)).fetchone()==(expected,)


def test_json_scalar_contract_and_reader_permissions(reader):
    for value,expected in [('abc','abc'),(42,'42'),(False,'false'),([],None),({},None),('x'*4001,None),(None,None)]:
        assert reader.execute('SELECT mart.json_scalar_4000(%s::jsonb,%s)',(json.dumps({'v':value}),'v')).fetchone()==(expected,)
    assert reader.execute("SELECT has_table_privilege(current_user,'dv.s_invoice','INSERT')").fetchone()==(False,)


def test_contract_drift_is_rejected_before_archive_reads(conn,archive,monkeypatch):
    monkeypatch.setenv('SNAPSHOT_COMMIT_LSN','ffffffff:ffffffff:ffff')
    with pytest.raises(ValueError,match='approved database contract'):
        load.run_once(archive,conn)


def test_new_entities_failure_rolls_back_links_and_scoped_ack(conn,archive):
    inv=unique_id()
    args=batch(archive,[sales_event('Invoices',inv,CustomerID=unique_id(),BillToCustomerID=unique_id(),OrderID=None)])
    with pytest.raises(RuntimeError):
        load.apply_batch(conn,*args,fail_after_stage=True)
    assert conn.execute('SELECT count(*) FROM dv.h_invoice WHERE invoice_id=%s',(inv,)).fetchone()==(0,)
    assert conn.execute('SELECT count(*) FROM ops.consumer_batches WHERE batch_id=%s',(args[0]['batch_id'],)).fetchone()==(0,)
    assert load.apply_batch(conn,*args)==1
