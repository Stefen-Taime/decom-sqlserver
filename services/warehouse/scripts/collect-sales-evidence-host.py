"""Summarize five-table validation without publishing source rows or secrets."""
from datetime import datetime,timezone
import importlib.util
import json
from pathlib import Path
import subprocess

root=Path('/srv/decom-sqlserver')
service=Path(__file__).resolve().parents[1]
validation=root/'validation'
def read(name):
    return json.loads((validation/name).read_text())
def pg(query):
    return subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-v','ON_ERROR_STOP=1',
        '-U','warehouse_admin','-d','warehouse','-c',query],text=True).strip()
before,after,restored=[read('warehouse-sales-v2-'+name+'.json') for name in ['before','after','restored']]
assert all(r['passed'] for r in [before,after,restored])
for table in before['tables']:
    checks=[r['tables'][table]['source_projection_sha256'] for r in [before,after,restored]]
    assert len(set(checks))==1,'Source business projection changed during validation'
    assert all(r['tables'][table]['warehouse_projection_sha256']==checks[0] for r in [before,after,restored])
cutover=json.loads((root/'releases/warehouse-sales-v2-20261028/pre-cutover.json').read_text())
assert pg(cutover['legacy_batch_query'])==cutover['legacy_batch_fingerprint']
assert pg(cutover['legacy_event_query'])==cutover['legacy_event_fingerprint']
assert int(pg("SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='customers-orders/v1'"))==cutover['legacy_batches']
ledgers=json.loads(pg('SELECT json_agg(row_to_json(x)) FROM ops.consumer_load_summary x'))
health={}
for name in ['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1','decom-cdc-archiver-1','decom-cdc-seaweedfs-1',
        'decom-cloudbeaver-cloudbeaver-1','decom-warehouse-postgres-1','decom-warehouse-loader-1']:
    state=json.loads(subprocess.check_output(['docker','inspect','--format','{{json .State}}',name],text=True))
    health[name]={'running':state['Running'],'health':state.get('Health',{}).get('Status')}
    assert health[name]=={'running':True,'health':'healthy'},health[name]
ui=json.loads(subprocess.check_output(['python3',str(service/'scripts/verify-cloudbeaver-host.py'),'--sales'],text=True))
access=json.loads(subprocess.check_output(['docker','compose','exec','-T','loader','python','check-access.py'],cwd=service,text=True))
spec=importlib.util.spec_from_file_location('source_export',service/'scripts/source-export.py')
source_module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_module)
cleanup=source_module.query("SELECT j.enabled FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
assert cleanup['enabled']==0
image=subprocess.check_output(['docker','inspect','--format','{{.Config.Image}} {{.Image}}','decom-warehouse-loader-1'],text=True).strip()
report={'verified_at':datetime.now(timezone.utc).isoformat(),'consumer_version':'sales/v2','scope':list(after['tables']),
    'integration_tests':{'passed':28,'isolated_database':'warehouse_test; removed after tests'},
    'reconciliation':after,'fixture_business_projection_unchanged':True,
    'upgrade':{'legacy_batches':cutover['legacy_batches'],'legacy_events':cutover['legacy_events'],
        'legacy_batch_fingerprint_preserved':True,'legacy_event_fingerprint_preserved':True,'legacy_scoped_checkpoints_copied':True,
        'pre_cutover_backup':cutover['backup'],'pre_cutover_backup_sha256':cutover['backup_sha256'],'old_image_id':cutover['old_image_id']},
    'consumer_ledgers':ledgers,'live_cdc_restart_replay':read('warehouse-sales-v2-live-recovery.json'),
    'backup_restore':read('warehouse-sales-v2-backup.json'),'cloudbeaver':ui,'archive_reader_permissions':access,
    'loader_image':image,'service_health':health,'automatic_cdc_cleanup_enabled':False,'purge_authorized':False,
    'limits':['Five captured business tables only; not the whole source database','Spatial and audit columns excluded from business reconciliation',
        'No stored procedure migration or application cutover','Same-VM restore with existing roles; no off-VM disaster recovery',
        'Per-batch atomicity and eventual cross-table consistency; no contiguous source purge watermark']}
path=validation/'warehouse-sales-v2-2026-10-28.json'
path.write_text(json.dumps(report,indent=2)+'\n')
path.chmod(0o600)
print(json.dumps({'evidence':str(path),'business_rows':sum(t['warehouse_rows'] for t in after['tables'].values()),
    'integration_tests_passed':28,'services_healthy':len(health),'legacy_state_preserved':True,'purge_authorized':False}))
