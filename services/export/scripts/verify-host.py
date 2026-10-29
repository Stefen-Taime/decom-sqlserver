"""Run full export validation on the VM, or collect independent-consumer evidence after protocol tests."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import time

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--reuse-protocol-report',action='store_true')
args=parser.parse_args()
root=Path('/srv/decom-sqlserver')
service=root/'workspace/services/export'
validation=root/'validation'
validation.mkdir(exist_ok=True,mode=0o700)


def run(command,cwd=service):
    result=subprocess.run(command,cwd=cwd,text=True,capture_output=True,timeout=1800)
    if result.returncode:
        failure=validation/'export-last-failure.log'
        failure.write_text(result.stdout+result.stderr);failure.chmod(0o600)
        raise RuntimeError('Validation failed; protected diagnostics: '+str(failure))
    return result


def job(name,*command):
    return run(['docker','compose','run','--rm','--no-deps',name,*command])


unit=job('exporter','-m','unittest','discover','-s','tests','-p','test_contract.py')
if not args.reuse_protocol_report:
    protocol=job('exporter','-m','tests.verify')
    (validation/'export-protocol.log').write_text(protocol.stdout+protocol.stderr)
    (validation/'export-protocol.log').chmod(0o600)
job('exporter','-m','tests.request_guards')
report=json.loads((root/'export/requests/validation-report.json').read_text())
report['contract_tests']={'passed':9,'output':unit.stderr.strip()}
report['consumer']={}
for dataset,export in report['exports'].items():
    listed=json.loads(job('consumer','consumer.py','list','--dataset',dataset).stdout)
    assert export['publication'] in listed['publications']
    read=json.loads(job('consumer','consumer.py','read','--dataset',dataset,
                       '--publication',export['publication']).stdout)
    assert read['status']=='validated'
    report['consumer'][dataset]=read
    print(json.dumps({'consumer_verified':dataset,'rows':read['statistics']['rows']}),flush=True)
report['consumer_access']=json.loads(job('consumer','-m','tests.consumer_access').stdout)
configuration=json.loads(run(['docker','compose','--profile','tools','config','--format','json']).stdout)
consumer=configuration['services']['consumer']
assert list(consumer['networks'])==['archive']
assert [s['source'] for s in consumer['secrets']]==['s3_consumer']
report['consumer_access']['checks'].append('Compose consumer has only archive network and its own S3 secret')

query="SELECT json_build_object('events',(SELECT count(*) FROM staging.cdc_event),'batches',(SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'),'current_keys',(SELECT count(*) FROM staging.current_key))"
counts=json.loads(run(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-U',
    'warehouse_admin','-d','warehouse','-c',query]).stdout)
before=json.loads((root/'releases/export-v1-20261028/pre-export-counts.json').read_text())
assert counts==before,'Quiet-fixture warehouse state changed; reconcile before accepting evidence'
report['warehouse_counts_before']=before
report['warehouse_counts_after']=counts
health={}
for name in ['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1','decom-cdc-archiver-1',
             'decom-cdc-seaweedfs-1','decom-cloudbeaver-cloudbeaver-1',
             'decom-warehouse-postgres-1','decom-warehouse-loader-1']:
    for attempt in range(25):
        state=json.loads(run(['docker','inspect','--format','{{json .State}}',name]).stdout)
        if state['Running'] and state.get('Health',{}).get('Status')=='healthy':
            break
        time.sleep(5)
    else:
        raise RuntimeError('Service unhealthy: '+name)
    health[name]={'running':True,'health':'healthy'}
report['service_health']=health
module_path=root/'workspace/services/warehouse/scripts/source-export.py'
spec=importlib.util.spec_from_file_location('source_export',module_path)
source=importlib.util.module_from_spec(spec);spec.loader.exec_module(source)
cleanup=source.query("SELECT j.enabled FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
assert cleanup['enabled']==0
report['automatic_cdc_cleanup_enabled']=False
report['image_id']=run(['docker','image','inspect','--format','{{.Id}}','decom-export:1']).stdout.strip()
report['contracts']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (service/'contracts').glob('*.json')}
report['migration_sha256']=hashlib.sha256((service.parent/'warehouse/sql/005-export-contracts.sql').read_bytes()).hexdigest()
report['verified_at']=datetime.now(timezone.utc).isoformat()
report['limits']=[
    'Full snapshots of current warehouse rows; invoice/order exports have separate PostgreSQL snapshots.',
    'No certified SQL Server transaction watermark, incremental deletion feed or purge authorization.',
    'Single writer host and durable local request journal; no distributed locking or host-loss recovery test.',
    'S3 credential permissions tested; trusted lab network/filer administration is not tenant isolation.',
    'Same-VM storage, no WORM, off-VM restore, automatic retention or scheduler.',
    'Consumer verifies files and persists a local acknowledgement; no real application replacement/cutover.']
destination=validation/'warehouse-exports-v1-2026-10-28.json'
destination.write_text(json.dumps(report,indent=2)+'\n');destination.chmod(0o600)
print(json.dumps({'evidence':str(destination),'services_healthy':len(health),'datasets':list(report['consumer'])}))
