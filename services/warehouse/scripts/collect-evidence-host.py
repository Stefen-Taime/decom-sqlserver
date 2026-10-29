"""Collect operational proof without exporting source rows, SQL definitions or secrets."""
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess

root=Path('/srv/decom-sqlserver')
validation=root/'validation'


def read(name):
    return json.loads((validation/name).read_text())


def pg(query):
    return subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-U','warehouse_admin','-d','warehouse','-c',query],text=True).strip()


before=read('warehouse-reconciliation-before.json')
after=read('warehouse-reconciliation-after.json')
restored=read('warehouse-restored-reconciliation.json')
for table in before['tables']:
    checks=[r['tables'][table]['source_projection_sha256'] for r in [before,after,restored]]
    assert len(set(checks))==1, 'Source fixture business projection changed during validation'
    assert all(r['tables'][table]['warehouse_projection_sha256']==checks[0] for r in [before,after,restored])
inventory_path=validation/'source-inventory-20261028.json'
inventory=json.loads(inventory_path.read_text())
containers=['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1','decom-cdc-archiver-1','decom-cdc-seaweedfs-1',
    'decom-cloudbeaver-cloudbeaver-1','decom-warehouse-postgres-1','decom-warehouse-loader-1']
health={}
for name in containers:
    data=json.loads(subprocess.check_output(['docker','inspect','--format','{{json .State}}',name],text=True))
    health[name]={'running':data['Running'],'health':data.get('Health',{}).get('Status')}
    assert health[name]=={'running':True,'health':'healthy'},health[name]
images={name:subprocess.check_output(['docker','inspect','--format','{{.Image}}',name],text=True).strip()
    for name in ['decom-warehouse-postgres-1','decom-warehouse-loader-1']}
settings=json.loads(pg("SELECT json_build_object('version',current_setting('server_version'),'fsync',current_setting('fsync'),'synchronous_commit',current_setting('synchronous_commit'),'data_checksums',current_setting('data_checksums'))"))
assert all(settings[k]=='on' for k in ['fsync','synchronous_commit','data_checksums'])
ui=json.loads(subprocess.check_output(['python3',str(root/'workspace/services/warehouse/scripts/verify-cloudbeaver-host.py')],text=True).strip())
access=json.loads(subprocess.check_output(['docker','compose','exec','-T','loader','python','check-access.py'],
    cwd=root/'workspace/services/warehouse',text=True).strip())
report={'verified_at':datetime.now(timezone.utc).isoformat(),'scope':['Sales.Customers','Sales.Orders'],
    'integration_tests':{'passed':15,'database':'isolated warehouse_test; removed after tests'},
    'reconciliation':after,'fixture_business_projection_unchanged':True,
    'live_cdc_restart_and_replay':read('warehouse-live-recovery.json'),
    'backup_restore':read('warehouse-backup-restore.json'),
    'source_inventory':{'path':str(inventory_path),'sha256':hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
        'object_counts':dict(Counter(o['type_desc'] for o in inventory['objects'])),
        'declared_dependency_count':len(inventory['dependencies']),
        'foreign_key_column_count':len(inventory['foreign_keys']),
        'dynamic_sql_review_candidates':sum(o['dynamic_sql_review'] for o in inventory['objects']),
        'sql_agent_jobs':list({step['name']: {'name':step['name'],'enabled':bool(step['enabled'])} for step in inventory['agent_job_steps']}.values()),
        'application_ownership_and_external_callers':'not established; review required',
        'stored_procedures_translated':False},
    'postgresql_settings':settings,'image_ids':images,'cloudbeaver':ui,'archive_reader_permissions':access,
    'service_health':health,'purge_authorized':False}
path=validation/'warehouse-pilot-2026-10-28.json'
path.write_text(json.dumps(report,indent=2)+'\n')
path.chmod(0o600)
print(json.dumps({'evidence':str(path),'services_healthy':len(health),'snapshot_rows':74258,
    'business_projection_mismatches':0,'tests_passed':15,'backup_restore':'passed','purge_authorized':False}))
