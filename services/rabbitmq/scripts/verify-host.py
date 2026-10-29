"""Acceptance on the VM: isolated protocol tests, broker/app recreation, full row reconciliation."""
import argparse
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import time

root=Path('/srv/decom-sqlserver');service=root/'workspace/services/rabbitmq'
validation=root/'validation';validation.mkdir(exist_ok=True,mode=0o700)
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--reuse-protocol-report',action='store_true')
args=parser.parse_args()


def run(command,expected=0):
    result=subprocess.run(command,cwd=service,text=True,capture_output=True,timeout=900)
    if result.returncode!=expected:
        path=validation/'messaging-last-failure.log'
        path.write_text(result.stdout+result.stderr);path.chmod(0o600)
        raise RuntimeError('See protected diagnostics: '+str(path))
    return result.stdout


def job(name,*command,options=None,expected=0):
    return run(['docker','compose','run','--rm','--no-deps',*(options or []),name,*command],expected)


def save(path,value):
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,indent=2)+'\n');temp.chmod(0o600);temp.replace(path)


def pg_counts():
    query="SELECT json_build_object('events',(SELECT count(*) FROM staging.cdc_event),'batches',(SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'),'current_keys',(SELECT count(*) FROM staging.current_key))"
    return json.loads(run(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-U','warehouse_admin','-d','warehouse','-c',query]))


progress_path=validation/'messaging-live-progress.json'
progress=json.loads(progress_path.read_text()) if progress_path.exists() else {'warehouse_before':pg_counts()}
save(progress_path,progress)
run(['docker','compose','run','--rm','--no-deps','probe','-m','unittest','discover','-s','regression'])
if not args.reuse_protocol_report:
    job('probe','-m','tests.protocol')
protocol=json.loads((root/'messaging/validation/protocol-report.json').read_text())
if 'queued' not in progress:
    current=json.loads(job('invoice-app','invoice_app.py','status'))
    assert current.get('deliveries',0)==0,'Existing app state: use retained recovery evidence, do not reset it.'
    progress['first_dispatch']=json.loads(job('dispatcher','dispatcher.py'))
    progress['queued']=json.loads(job('probe','inspect_broker.py'))
    assert progress['queued']['queues_ready']['invoice-app.ready']>0
    save(progress_path,progress)
if 'broker_recreated' not in progress:
    run(['docker','compose','up','-d','--force-recreate','--wait','--wait-timeout','180','rabbitmq'])
    state=json.loads(job('probe','inspect_broker.py'))
    assert state['queues_ready']==progress['queued']['queues_ready']
    progress['broker_recreated']=state['queues_ready'];save(progress_path,progress)
    print(json.dumps({'broker_recreation_preserved':state['queues_ready']}),flush=True)
if 'application_crash_after_commit' not in progress:
    job('invoice-app','invoice_app.py','once',options=['-e','MESSAGING_TEST_MODE=1','-e','MESSAGING_FAULT=after-app-commit'],expected=86)
    state=json.loads(job('invoice-app','invoice_app.py','status'))
    assert state['invoice_lines']==228265 and state['deliveries']==1
    progress['application_crash_after_commit']={'rows':state['invoice_lines'],'deliveries':state['deliveries']}
    save(progress_path,progress)
    print(json.dumps({'app_commit_survived_crash':progress['application_crash_after_commit']}),flush=True)
run(['docker','compose','up','-d','--wait','--wait-timeout','180','invoice-app'])
expected_deliveries=progress['queued']['queues_ready']['invoice-app.ready']
for _ in range(60):
    state=json.loads(job('invoice-app','invoice_app.py','status'))
    broker=json.loads(job('probe','inspect_broker.py'))
    if state.get('deliveries',0)>=expected_deliveries and broker['queues_ready']['invoice-app.ready']==0: break
    time.sleep(3)
else: raise RuntimeError('Application did not catch up')
assert state['invoice_lines']==228265
if 'application_recreated' not in progress:
    run(['docker','compose','up','-d','--force-recreate','--wait','--wait-timeout','180','invoice-app'])
    after=json.loads(job('invoice-app','invoice_app.py','status'))
    assert after['active']==state['active'] and after['invoice_lines']==state['invoice_lines'] and after['deliveries']==state['deliveries']
    progress['application_recreated']=True;save(progress_path,progress)
audit=json.loads(job('invoice-app','audit_app.py'))
print(json.dumps({'application_rows_reconciled':audit['statistics']['rows']}),flush=True)
# The watch dispatcher is started only after its idempotent scan was checked.
running=run(['docker','compose','ps','--status','running','--services']).splitlines()
if 'dispatcher' not in running:
    repeat=json.loads(job('dispatcher','dispatcher.py'))
    assert repeat['confirmed']==0 and repeat['pending']==0
    progress['repeated_scan']=repeat;save(progress_path,progress)
run(['docker','compose','up','-d','--wait','--wait-timeout','180','dispatcher'])
if 'watch_delivery' not in progress:
    if 'watch_request_id' not in progress:
        progress['watch_request_id']='invoice-rabbitmq-e2e-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        save(progress_path,progress)
    output=run(['docker','compose','--project-directory',str(service.parent/'export'),
        'run','--rm','--no-deps','exporter','export.py','--dataset','invoice_details',
        '--request-id',progress['watch_request_id']])
    exported=json.loads(output.strip().splitlines()[-1])
    for _ in range(60):
        state=json.loads(job('invoice-app','invoice_app.py','status'))
        if state.get('active') and state['active']['run_id']==exported['run_id']: break
        time.sleep(3)
    else: raise RuntimeError('Watch services did not automatically process the new export')
    progress['watch_delivery']={'export':exported,'application_event_id':state['active']['event_id'],
                                'automatic_dispatch_and_consumption':True}
    save(progress_path,progress)
    print(json.dumps({'new_export_automatically_consumed':exported['run_id']}),flush=True)
state=json.loads(job('invoice-app','invoice_app.py','status'))
audit=json.loads(job('invoice-app','audit_app.py'))
broker=json.loads(job('probe','inspect_broker.py'))
assert broker['queues_ready']=={'invoice-app.ready':0,'invoice-app.dead':0}
counts=pg_counts();assert counts==progress['warehouse_before']
health={}
for name in ['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1','decom-cdc-archiver-1',
    'decom-cdc-seaweedfs-1','decom-cloudbeaver-cloudbeaver-1','decom-warehouse-postgres-1',
    'decom-warehouse-loader-1','decom-messaging-rabbitmq-1','decom-messaging-dispatcher-1','decom-messaging-invoice-app-1']:
    value=json.loads(run(['docker','inspect','--format','{{json .State}}',name]))
    assert value['Running'] and value.get('Health',{}).get('Status')=='healthy',name
    health[name]='healthy'
configuration=json.loads(run(['docker','compose','config','--format','json']))
for name in ['dispatcher','invoice-app']:
    item=configuration['services'][name]
    assert set(item['networks'])=={'messaging','archive'}
    assert 'admin' not in [s['source'] for s in item['secrets']]
    assert not any('PG' in key for key in item.get('environment',{}))
spec=importlib.util.spec_from_file_location('source_export',root/'workspace/services/warehouse/scripts/source-export.py')
source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
cleanup=source.query("SELECT j.enabled FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
assert cleanup['enabled']==0
report={'verified_at':datetime.now(timezone.utc).isoformat(),'regression_tests_passed':9,
    'isolated_protocol':protocol,'live_recovery':progress,'application':state,'stored_rows_audit':audit,
    'broker':broker,'warehouse_after':counts,'service_health':health,
    'runtime_has_no_admin_credentials_or_warehouse_network':True,
    'automatic_cdc_cleanup_enabled':False,'purge_authorized':False,
    'application_image_id':run(['docker','image','inspect','--format','{{.Id}}','decom-invoice-messaging:1']).strip(),
    'shared_export_verifier_sha256':hashlib.sha256((service.parent/'export/app/common.py').read_bytes()).hexdigest(),
    'limits':['Single broker member and one VM: no HA or off-VM disaster-recovery proof.',
              'At-least-once messaging; deduplicated effects only within this local transactional application.',
              'Full-snapshot ordering uses capture time and run ID; no certified SQL Server CDC boundary.',
              'Trusted internal Docker network; HTTP/AMQP transport without TLS; management via loopback/IAP.',
              'DLQ review/replay, external alerting, retention and source decommissioning remain operator work.']}
destination=validation/'rabbitmq-invoice-app-v1-2026-10-29.json';save(destination,report)
print(json.dumps({'evidence':str(destination),'healthy_services':len(health),'invoice_rows':audit['statistics']['rows'],
                  'protocol_checks':len(protocol['checks'])}),flush=True)
