"""Behavioral tests: exact boundaries, current rows, reference versions, money and privileges."""
from copy import deepcopy
from decimal import Decimal
from io import BytesIO
import json
import os
from pathlib import Path
import sys
import psycopg
import pytest
from botocore.exceptions import ClientError

sys.path.insert(0,'/app')
from archive import ArchiveStore
from load import apply_batch,connect,verified_batch

VERSION='test-package/v1'
LOW='2026-10-28T10:00:00.1234566'
POINT='2026-10-28T10:00:00.1234567'
HIGH='2026-10-28T10:00:00.1234568'


class MemoryS3:
    def __init__(self): self.objects={}
    def get_object(self,Key,**kwargs):
        if Key not in self.objects:
            raise ClientError({'Error':{'Code':'NoSuchKey'}},'GetObject')
        return {'Body':BytesIO(self.objects[Key])}
    def put_object(self,Key,Body,**kwargs): self.objects[Key]=Body


def event(table,row,sequence=1,op='c'):
    return {'payload':{'source':{'db':'WideWorldImporters','schema':'Sales','table':table,'snapshot':'false',
        'commit_lsn':f'00000001:00000001:{sequence:04x}','change_lsn':f'00000001:00000001:{sequence:04x}',
        'event_serial_no':2 if op=='u' else 1,'ts_ms':1793203200000},'ts_ms':1793203200000,
        'op':op,'before':deepcopy(row) if op in {'u','d'} else None,'after':None if op=='d' else row}}


def load_events(events):
    archive=ArchiveStore(MemoryS3(),'cdc-raw')
    result=archive.archive(json.dumps(events).encode())
    receipt,checksum,values=verified_batch(archive,result['receipt_key'])
    with connect() as conn:
        apply_batch(conn,receipt,result['receipt_key'],checksum,values)


@pytest.fixture(scope='module',autouse=True)
def seed():
    assert os.environ['PGDATABASE']=='warehouse_sp_test'
    with psycopg.connect(host='postgres',dbname='warehouse_sp_test',user='warehouse_admin',password=Path('/run/secrets/admin').read_text().strip(),autocommit=True) as admin:
        admin.execute("INSERT INTO ops.reference_snapshots VALUES(%s,'Warehouse.PackageTypes','[]',%s,now(),1,now(),'explicit_versioned_snapshot')",(VERSION,'0'*64))
        admin.execute("INSERT INTO ref.package_types VALUES(%s,1,'Carton','2013-01-01','9999-12-31')",(VERSION,))
        admin.execute("INSERT INTO ops.reference_snapshots VALUES('incomplete','Warehouse.PackageTypes','[]',%s,now(),2,now(),'explicit_versioned_snapshot')",('1'*64,))
        admin.execute("INSERT INTO ops.reference_snapshots VALUES('wrong-content','Warehouse.PackageTypes','[]',%s,now(),1,now(),'explicit_versioned_snapshot')",('2'*64,))
        admin.execute("INSERT INTO ref.package_types VALUES('wrong-content',99,'Other','2013-01-01','9999-12-31')")
    customer={'CustomerID':1,'CustomerName':'Fixture','BillToCustomerID':1,'CreditLimit':None,'IsOnCreditHold':False,'DeliveryCityID':77}
    order={'OrderID':1,'CustomerID':1,'OrderDate':'2026-10-01Z','ExpectedDeliveryDate':'2026-10-02Z','LastEditedWhen':POINT+'Z',
        'SalespersonPersonID':3,'PickedByPersonID':None,'BackorderOrderID':None}
    line={'OrderLineID':10,'OrderID':1,'StockItemID':5,'PackageTypeID':1,'Description':'Positive half cent','Quantity':1,
        'UnitPrice':'0.05','TaxRate':'10.000','PickedQuantity':0,'PickingCompletedWhen':None,'LastEditedWhen':LOW+'Z'}
    load_events([event('Customers',customer),event('Orders',order),event('OrderLines',line),
        event('OrderLines',{**line,'OrderLineID':11,'Description':'Negative half cent','Quantity':-1}),
        event('OrderLines',{**line,'OrderLineID':12,'Description':'Null price','UnitPrice':None}),
        event('OrderLines',{**line,'OrderLineID':13,'Description':'Child newer','LastEditedWhen':HIGH+'Z','PickingCompletedWhen':HIGH+'Z'})])


@pytest.fixture
def reader():
    with psycopg.connect(host='postgres',dbname='warehouse_sp_test',user='warehouse_reader',
            password=Path('/run/secrets/reader').read_text().strip(),autocommit=True) as conn:
        yield conn


def rows(reader,lower,upper,version=VERSION):
    return reader.execute('SELECT * FROM app.get_order_updates(%s,%s,%s)',(lower,upper,version)).fetchall()


def test_exact_seventh_digit_open_lower_closed_upper_and_parent_change(reader):
    result=rows(reader,LOW,POINT)
    assert len(result)==3
    assert all(r[-1]==POINT for r in result)
    assert len(rows(reader,POINT,HIGH))==1
    assert rows(reader,POINT,POINT)==[]
    assert rows(reader,HIGH,LOW)==[]


def test_money_nulls_and_source_shape(reader):
    result=rows(reader,LOW,HIGH)
    assert len(result)==4 and all(len(r)==18 for r in result)
    by_name={r[4]:r for r in result}
    assert by_name['Positive half cent'][9:12]==(Decimal('.05'),Decimal('.01'),Decimal('.06'))
    assert by_name['Negative half cent'][9:12]==(Decimal('-.05'),Decimal('-.01'),Decimal('-.06'))
    assert by_name['Null price'][7] is None and by_name['Null price'][9:12]==(None,None,None)
    assert by_name['Child newer'][1].isoformat()=='2026-10-28'
    assert all(r[5]=='Carton' and r[12]==77 for r in result)


@pytest.mark.parametrize('lower,upper',[(None,HIGH),(LOW,None),(None,None)])
def test_null_cutoff_means_empty_result(reader,lower,upper):
    assert rows(reader,lower,upper)==[]


@pytest.mark.parametrize('value',['2026-02-29','0000-01-01','2026-10-28T24:00:00','2026-10-28T10:00:00.12345678',
    '2026-10-28T10:00:00Z',"'; DROP SCHEMA dv CASCADE; --"])
def test_invalid_cutoff_is_rejected(reader,value):
    with pytest.raises(psycopg.DataError): rows(reader,value,HIGH)


def test_date_only_input_is_midnight_without_rounding(reader):
    assert reader.execute("SELECT app.datetime2_7('2026-10-28')").fetchone()==('2026-10-28T00:00:00.0000000',)
    assert reader.execute('SELECT app.datetime2_7(%s)',(POINT,)).fetchone()==(POINT,)


@pytest.mark.parametrize('version',['unknown','incomplete',None])
def test_unknown_or_incomplete_reference_rejected(reader,version):
    with pytest.raises(psycopg.errors.InvalidParameterValue): rows(reader,LOW,HIGH,version)


def test_missing_reference_key_does_not_silently_drop_output(reader):
    with pytest.raises(psycopg.errors.ForeignKeyViolation): rows(reader,LOW,HIGH,'wrong-content')


def test_no_public_execute_no_loader_execute_and_no_reader_writes(reader):
    assert reader.execute("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE n.nspname='app' AND a.grantee=0 AND a.privilege_type='EXECUTE'").fetchone()==(0,)
    assert reader.execute("SELECT has_function_privilege('warehouse_loader','app.get_order_updates(text,text,text)','EXECUTE'),has_table_privilege(current_user,'ref.package_types','INSERT')").fetchone()==(False,False)
    assert reader.execute("SELECT bool_and(NOT p.prosecdef) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='app'").fetchone()==(True,)


def test_overflow_matches_fixed_source_decimal_bounds(reader):
    with pytest.raises(psycopg.errors.NumericValueOutOfRange):
        reader.execute('SELECT * FROM app.order_line_amounts(%s,%s,%s)',(2147483647,Decimal('9999999999999999.99'),Decimal('999999999999999.999')))


@pytest.mark.parametrize('quantity',[1,-1])
@pytest.mark.parametrize('rate,expected',[('49.999','0'),('50.000','.01'),('50.001','.01')])
def test_source_division_scale_before_commercial_rounding(reader,quantity,rate,expected):
    result=reader.execute('SELECT tax_amount FROM app.order_line_amounts(%s,.01,%s)',(quantity,Decimal(rate))).fetchone()[0]
    assert result==quantity*Decimal(expected)


def test_delete_is_absent_from_current_extraction(reader):
    row={'OrderLineID':20,'OrderID':1,'StockItemID':5,'PackageTypeID':1,'Description':'Deleted','Quantity':1,
        'UnitPrice':'1.00','TaxRate':'15.000','PickedQuantity':0,'PickingCompletedWhen':None,'LastEditedWhen':'2026-10-29T00:00:00.0000001Z'}
    load_events([event('OrderLines',row),event('OrderLines',row,2,'d')])
    assert rows(reader,'2026-10-29','2026-10-30')==[]
