"""Real four-entity CDC transitions, scope-safe replay and durable PostgreSQL restart."""
from datetime import datetime,timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

os.umask(0o077)
service=Path(__file__).resolve().parents[1]
root=Path('/srv/decom-sqlserver')
token='sales-v2-'+str(uuid.uuid4())
key=2000000100
tables={'Orders':'OrderID','OrderLines':'OrderLineID','Invoices':'InvoiceID','InvoiceLines':'InvoiceLineID'}
inventory=json.loads((root/'validation/source-inventory-20261028.json').read_text())
spec=importlib.util.spec_from_file_location('source_export',service/'scripts/source-export.py')
source_module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_module)


def compose(*args):
    result=subprocess.run(['docker','compose',*args],cwd=service,text=True,capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout+result.stderr)
    return result.stdout


def pg(sql):
    return json.loads(subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-v','ON_ERROR_STOP=1',
        '-U','warehouse_admin','-d','warehouse','-c',sql],text=True))


def source_sql(sql):
    result=subprocess.run(['docker','exec','-i','decom-sqlserver-sqlserver-1','/bin/bash','/opt/lab/bin/sqlcmd.sh'],
        input='USE WideWorldImporters; SET NOCOUNT ON; SET XACT_ABORT ON; '
        'SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; '
        'SET CONCAT_NULL_YIELDS_NULL ON; SET ARITHABORT ON; SET NUMERIC_ROUNDABORT OFF;\n'+sql,
        text=True,capture_output=True)
    if result.returncode:
        raise RuntimeError('Synthetic sales probe failed: '+result.stdout+result.stderr)


def fields(phase):
    fraction='1234567' if phase=='created' else '7654321'
    delivery=json.dumps({'DeliveredWhen':'2026-10-28T12:30:15.'+fraction,'ReceivedBy':token+'-'+phase})
    return delivery.replace("'","''")


def current_invoice():
    return pg(f"""SELECT coalesce((SELECT json_build_object('CustomerID',customer_id,'BillToCustomerID',bill_to_customer_id,
        'OrderID',order_id,'ConfirmedDeliveryTime',confirmed_delivery_time,'ConfirmedReceivedBy',confirmed_received_by)
        FROM mart.invoices WHERE invoice_id={key}),'null'::json)""")


def wait_for(predicate,label):
    deadline=time.monotonic()+240
    while time.monotonic()<deadline:
        if predicate():
            print(label+': PASS',flush=True)
            return
        time.sleep(2)
    raise RuntimeError('Timed out waiting for '+label)


def counts():
    parts={
        **{view:f'SELECT count(*) FROM mart.{view}' for view in ['customers','orders','order_lines','invoices','invoice_lines']},
        'journal_events':'SELECT count(*) FROM staging.cdc_event','batch_headers':'SELECT count(*) FROM ops.archive_batches',
        'scope_batches':"SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'"}
    for table in ['h_customer','h_order','h_order_line','h_invoice','h_invoice_line','l_order_customer','l_order_line_order',
            'l_invoice_customer','l_invoice_bill_to_customer','l_invoice_order','l_invoice_line_invoice','s_customer','s_order','s_order_line','s_invoice','s_invoice_line']:
        parts[table]=f'SELECT count(*) FROM dv.{table}'
    return pg('SELECT json_build_object('+','.join("'"+name+"',("+sql+")" for name,sql in parts.items())+')')


def main():
    if pg(f"SELECT count(*) FROM staging.cdc_event WHERE source_pk={key}"):
        raise RuntimeError('Reserved probe key already has warehouse history; choose a new key explicitly')
    guard='BEGIN TRANSACTION;\n'+''.join(f"IF EXISTS(SELECT 1 FROM Sales.{table} WITH(UPDLOCK,HOLDLOCK) WHERE {pk}={key}) THROW 51000,'Reserved probe key occupied',1;\n" for table,pk in tables.items())
    overrides={
        'Orders':{'OrderID':str(key),'Comments':"N'"+token+"'"},
        'OrderLines':{'OrderLineID':str(key),'OrderID':str(key),'Description':"N'"+token+"'",'Quantity':'2','PickedQuantity':'0','UnitPrice':'0.10','TaxRate':'7.500'},
        'Invoices':{'InvoiceID':str(key),'OrderID':'NULL','CustomerID':'1','BillToCustomerID':'2','Comments':"N'"+token+"'",'ReturnedDeliveryData':"N'"+fields('created')+"'"},
        'InvoiceLines':{'InvoiceLineID':str(key),'InvoiceID':str(key),'Description':"N'"+token+"'",'Quantity':'2','UnitPrice':'0.10','TaxRate':'7.500','TaxAmount':'0.03','LineProfit':'0.01','ExtendedPrice':'0.23'}
    }
    for table,pk in tables.items():
        columns=[c['name'] for c in inventory['columns'] if c['schema_name']=='Sales' and c['table_name']==table and not c['is_computed']]
        overrides[table]['LastEditedWhen']='SYSUTCDATETIME()'
        guard+=f"INSERT Sales.{table} ("+','.join('['+c+']' for c in columns)+') SELECT '+','.join(overrides[table].get(c,'['+c+']') for c in columns)+f' FROM Sales.{table} WHERE {pk}=1;\n'
        guard+="IF @@ROWCOUNT<>1 THROW 51000,'Probe template missing',1;\n"
    created=False
    phases={}
    try:
        source_sql(guard+'COMMIT;')
        created=True
        for phase in ['created','updated']:
            if phase=='updated':
                source_sql(f"""BEGIN TRANSACTION;
                UPDATE Sales.OrderLines SET Quantity=3,LastEditedWhen=SYSUTCDATETIME() WHERE OrderLineID={key};
                UPDATE Sales.Invoices SET OrderID={key},ReturnedDeliveryData=N'{fields(phase)}',LastEditedWhen=SYSUTCDATETIME() WHERE InvoiceID={key};
                UPDATE Sales.InvoiceLines SET Quantity=3,TaxAmount=0.04,ExtendedPrice=0.34,LastEditedWhen=SYSUTCDATETIME() WHERE InvoiceLineID={key};
                COMMIT;""")
            expected=source_module.query(f'SELECT CustomerID,BillToCustomerID,OrderID,ConfirmedDeliveryTime,ConfirmedReceivedBy FROM Sales.Invoices WHERE InvoiceID={key} FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;')
            expected_qty=2 if phase=='created' else 3
            expected_price='0.23' if phase=='created' else '0.34'
            def reached():
                if current_invoice()!=expected:
                    return False
                values=pg(f"SELECT coalesce((SELECT json_build_array(quantity,extended_price::text) FROM mart.invoice_lines WHERE invoice_line_id={key}),'null'::json)")
                order_qty=pg(f"SELECT coalesce((SELECT quantity FROM mart.order_lines WHERE order_line_id={key}),-1)")
                return values==[expected_qty,expected_price] and order_qty==expected_qty
            wait_for(reached,'Live '+phase+' state including derived invoice columns')
            phases[phase]={'invoice':expected,'line_quantity':expected_qty,'line_extended_price':expected_price}
        source_sql('BEGIN TRANSACTION;'+''.join(f'DELETE Sales.{table} WHERE {pk}={key};' for table,pk in reversed(list(tables.items())))+'COMMIT;')
        created=False
        wait_for(lambda:pg(f"SELECT count(*) FROM staging.current_key k JOIN staging.cdc_event e USING(event_id) WHERE k.source_pk={key} AND e.operation='d'")==4,'Live deletes for all four entities')
        history=pg(f"""SELECT json_object_agg(table_name,operations) FROM
          (SELECT table_name,json_agg(operation ORDER BY commit_lsn,change_lsn,event_serial_no) AS operations
           FROM staging.cdc_event WHERE source_pk={key} GROUP BY table_name) x""")
        assert history=={'Sales.Orders':['c','d'],'Sales.OrderLines':['c','u','d'],'Sales.Invoices':['c','u','d'],'Sales.InvoiceLines':['c','u','d']},history
        # The streaming raw computed fields are NULL; the query view derived the verified values.
        null_computed=pg(f"SELECT count(*) FROM staging.cdc_event WHERE table_name='Sales.Invoices' AND source_pk={key} AND operation IN ('c','u') AND row_data->>'ConfirmedDeliveryTime' IS NULL AND row_data->>'ConfirmedReceivedBy' IS NULL")
        assert null_computed==2,null_computed
        before=counts()
        assert [before[k] for k in ['customers','orders','order_lines','invoices','invoice_lines']]==[663,73595,231412,70510,228265]
        compose('stop','loader')
        try:
            compose('up','-d','--no-deps','--force-recreate','--wait','--wait-timeout','180','postgres')
            after_restart=counts()
            assert before==after_restart
            print('PostgreSQL recreation retained the five-table state: PASS',flush=True)
            replay=json.loads(compose('run','--rm','--no-deps','backfill','python','load.py','--replay').strip().splitlines()[-1])
            assert replay['new_events']==0,replay
            after_replay=counts()
            assert before==after_replay
            print('Five-table archive replay added no events or vault rows: PASS',flush=True)
        finally:
            compose('up','-d','--wait','--wait-timeout','360','loader')
        result={'verified_at':datetime.now(timezone.utc).isoformat(),'probe_key':key,'marker':token,'phases':phases,
            'operations':history,'raw_streaming_invoice_computed_values_null':True,'computed_values_rebuilt_from_delivery_json':True,
            'counts_before_restart':before,'counts_after_restart':after_restart,'counts_after_replay':after_replay,'replay':replay,
            'source_probe_rows_removed':True,'purge_authorized':False}
        (root/'validation/warehouse-sales-v2-live-recovery.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2))
    finally:
        if created:
            # Delete only rows bearing this run's marker, children before parents.
            cleanup='BEGIN TRANSACTION;'
            for table,pk in reversed(list(tables.items())):
                column='Description' if table.endswith('Lines') else 'Comments'
                cleanup+=f"DELETE Sales.{table} WHERE {pk}={key} AND {column}=N'{token}';"
            source_sql(cleanup+'COMMIT;')


if __name__=='__main__':
    main()
