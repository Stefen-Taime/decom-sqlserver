"""Compare the deployed read API, validate money semantics and retain sanitized evidence."""
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

os.umask(0o077)
root=Path('/srv/decom-sqlserver')
service=Path(__file__).resolve().parents[1]
directory=root/'validation/sql-objects-20261028'
source_results=directory/'order-updates-results'
spec=importlib.util.spec_from_file_location('source_export',service/'scripts/source-export.py')
source=importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


def pg(query):
    return subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-v','ON_ERROR_STOP=1',
        '-U','warehouse_admin','-d','warehouse','-c',query],text=True).strip()


def reader_command():
    return ['docker','compose','run','--rm','--no-deps','-T','--user','0:0','-e','PGUSER=warehouse_reader',
        '-e','PGPASSWORD_FILE=/run/secrets/reader','-v',str(root/'secrets/warehouse/reader_password')+':/run/secrets/reader:ro',
        '-v',str(service/'scripts')+':/validation:ro','backfill']


counts_query="SELECT json_build_object('events',(SELECT count(*) FROM staging.cdc_event),'scope_batches',(SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'),'current_keys',(SELECT count(*) FROM staging.current_key))"
before=json.loads(pg(counts_query))
report_path=directory/'order-updates-comparison.json'
with report_path.open('w') as output:
    subprocess.run(reader_command()[:-1]+['-v',str(source_results)+':/input:ro','backfill','python','/validation/compare-order-updates.py','/input'],
        cwd=service,stdout=output,text=True,check=True)
comparison=json.loads(report_path.read_text())
assert comparison['passed']
print('Seven real source-procedure comparisons: PASS',flush=True)

# Obtain golden rounding/NULL results from SQL Server itself, with the source column types.
money_cases=[(1,'0.05','10.000'),(-1,'0.05','10.000'),(3,'0.10','7.500'),(2,None,'15.000'),
    (2147483647,'9999999999999999.99','0.000'),(1,'0.01','49.999'),(1,'0.01','50.000'),(1,'0.01','50.001'),
    (-1,'0.01','49.999'),(-1,'0.01','50.000'),(-1,'0.01','50.001'),(2147483647,'9999999999999999.99','49.999')]
values=','.join(f"({i},{q},{'NULL' if p is None else p},{t})" for i,(q,p,t) in enumerate(money_cases))
# Fetch SQL decimal values as strings; never route exact monetary values through floats.
golden=source.query(f"""SELECT JSON_QUERY((SELECT n,CONVERT(varchar(80),ROUND(q*p,2)) AS net,CONVERT(varchar(80),ROUND(q*p*t/100.0,2)) AS tax,
 CONVERT(varchar(80),ROUND(q*p,2)+ROUND(q*p*t/100.0,2)) AS total FROM (SELECT n,CAST(q AS int) q,CAST(p AS decimal(18,2)) p,CAST(t AS decimal(18,3)) t
 FROM (VALUES {values}) v(n,q,p,t)) typed ORDER BY n FOR JSON PATH,INCLUDE_NULL_VALUES)) AS results
 FOR JSON PATH,WITHOUT_ARRAY_WRAPPER""")
code="""import json,sys
from decimal import Decimal
sys.path.insert(0,'/app')
from load import connect
cases=json.loads(sys.argv[1]); result=[]
with connect() as conn:
 for i,(q,p,t) in enumerate(cases):
  row=conn.execute('SELECT * FROM app.order_line_amounts(%s,%s,%s)',(q,None if p is None else Decimal(p),Decimal(t))).fetchone()
  result.append({'n':i,**{k:None if v is None else str(v) for k,v in zip(['net','tax','total'],row)}})
print(json.dumps({'results':result}))
"""
target=json.loads(subprocess.check_output(reader_command()+['python','-c',code,json.dumps(money_cases)],cwd=service,text=True))
for expected,actual in zip(golden['results'],target['results']):
    assert expected['n']==actual['n']
    differences=[k for k in ['net','tax','total'] if (None if expected[k] is None else Decimal(expected[k]))!=(None if actual[k] is None else Decimal(actual[k]))]
    assert not differences,{'case':expected['n'],'differing_columns':differences}
assert len(target['results'])==len(money_cases)
overflow=source.query("""BEGIN TRY
 DECLARE @q int=2147483647,@p decimal(18,2)=9999999999999999.99,@t decimal(18,3)=999999999999999.999;
 DECLARE @r decimal(38,6)=ROUND(@q*@p*@t/100.0,2);
 SELECT 0 AS overflow_detected FOR JSON PATH,WITHOUT_ARRAY_WRAPPER;
 END TRY BEGIN CATCH SELECT ERROR_NUMBER() AS error_number,1 AS overflow_detected FOR JSON PATH,WITHOUT_ARRAY_WRAPPER; END CATCH;""")
assert overflow=={'error_number':8115,'overflow_detected':1},overflow
print('Twelve SQL Server numeric golden cases and overflow reference: PASS',flush=True)

assert json.loads(pg(counts_query))==before,'Read API validation changed warehouse ingestion state'
reference=json.loads((directory/'package-types.json').read_text())
catalog=json.loads((directory/'catalog.json').read_text())
current=source.query("SELECT OBJECT_DEFINITION(OBJECT_ID(N'Integration.GetOrderUpdates')) AS definition,JSON_QUERY((SELECT PackageTypeID,PackageTypeName,ValidFrom,ValidTo FROM Warehouse.PackageTypes ORDER BY PackageTypeID FOR JSON PATH,INCLUDE_NULL_VALUES)) AS packages,CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)) AS engine_version,(SELECT compatibility_level FROM sys.databases WHERE name=DB_NAME()) AS compatibility_level FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
assert hashlib.sha256(current['definition'].encode()).hexdigest()==comparison['source_definition_sha256']
assert hashlib.sha256(json.dumps(current['packages'],sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()==reference['sha256']
health={}
for name in ['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1','decom-cdc-archiver-1','decom-cdc-seaweedfs-1',
        'decom-cloudbeaver-cloudbeaver-1','decom-warehouse-postgres-1','decom-warehouse-loader-1']:
    state=json.loads(subprocess.check_output(['docker','inspect','--format','{{json .State}}',name],text=True))
    health[name]={'running':state['Running'],'health':state.get('Health',{}).get('Status')}
    assert health[name]=={'running':True,'health':'healthy'}
report={'verified_at':datetime.now(timezone.utc).isoformat(),'pilot':'Integration.GetOrderUpdates',
    'target':'app.get_order_updates(text,text,text)','source_engine':{'version':current['engine_version'],'compatibility_level':current['compatibility_level']},
    'isolated_integration_tests_passed':25,'source_procedure_comparison':comparison,
    'numeric_golden_cases':{'passed':len(money_cases),'source_overflow_error':8115,'target_overflow_sqlstate':'22003 (isolated test)'},
    'reference':{'version':reference['version'],'rows':len(reference['rows']),'automatic_refresh':False},
    'source_procedure_definition_unchanged':True,'warehouse_ingestion_state_unchanged':before,
    'migration_checksums':json.loads(pg('SELECT json_agg(row_to_json(x)) FROM (SELECT version,sha256 FROM ops.schema_migrations ORDER BY version) x')),
    'services':health,'application_cutover_authorized':False,'source_retirement_authorized':False,'purge_authorized':False}
ui=json.loads(subprocess.check_output(['python3',str(service/'scripts/verify-cloudbeaver-host.py'),'--order-updates'],text=True))
report['cloudbeaver']=ui
(root/'validation/sql-object-pilot-2026-10-28.json').write_text(json.dumps(report,indent=2)+'\n')
for item in catalog['procedures']:
    if item['source_object']=='Integration.GetOrderUpdates':
        item.update(migration_status='migrated_lab_pilot',target='app.get_order_updates(text,text,text)',
            validation_evidence='docs/validation/sql-object-pilot-2026-10-28.json',reference_mode='explicit versioned PackageTypes snapshot; automatic refresh not implemented')
(root/'validation/sql-object-catalog-2026-10-28.json').write_text(json.dumps(catalog,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'evidence':str(root/'validation/sql-object-pilot-2026-10-28.json'),'catalog_procedures':len(catalog['procedures']),
    'full_result_rows':comparison['cases']['full_current']['target_rows'],'source_cases_passed':7,'isolated_tests_passed':25,'services_healthy':len(health)}))
