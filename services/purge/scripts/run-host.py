"""Run a bounded, read-only CDC audit and retain an immutable report for this observation."""
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid
from source import collect, query

ROOT=Path('/srv/decom-sqlserver')
SERVICE=ROOT/'workspace/services/purge'
sys.path.insert(0,str(SERVICE/'app'))
from audit_core import decision


def save(path,value,owner=0):
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as stream:
        temporary=Path(stream.name)
        stream.write((json.dumps(value,indent=2)+'\n').encode())
        stream.flush();os.fsync(stream.fileno())
    os.chown(temporary,owner,owner);temporary.chmod(0o400)
    os.replace(temporary,path)
    descriptor=os.open(path.parent,os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def fingerprint(source):
    value={'upper_lsn':source['upper_lsn'],'capture_instances':source['capture_instances'],'captured_columns':source['captured_columns'],
           'tables':{table:sorted((json.dumps(row,sort_keys=True) for row in rows)) for table,rows in source['tables'].items()}}
    return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()


def main():
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the VM.')
    policy=json.loads((SERVICE/'policy.json').read_text())
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:12]
    directory=ROOT/'purge/runs'/run_id
    directory.mkdir(mode=0o700)
    for name in ['input','output']:
        path=directory/name;path.mkdir(mode=0o700);os.chown(path,10010,10010)
    environment={**os.environ,'PURGE_RUN_DIR':str(directory)}
    spec=importlib.util.spec_from_file_location('source_admin',ROOT/'workspace/services/warehouse/scripts/source-export.py')
    admin=importlib.util.module_from_spec(spec);spec.loader.exec_module(admin)
    def control():
        return admin.query("SELECT JSON_QUERY((SELECT j.enabled,c.retention FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH)) AS cleanup_jobs FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
    def job(*arguments):
        command=['docker','compose','run','--rm','--no-deps','audit',*arguments]
        result=subprocess.run(command,cwd=SERVICE,env=environment,text=True,capture_output=True,timeout=1800)
        name='audit' if not arguments else 'check-'+str(uuid.uuid4())[:8]
        log=directory/(name+'.log');log.write_text(result.stdout+result.stderr);log.chmod(0o600)
        if result.returncode: raise RuntimeError('Audit command failed; see '+str(log))
        return result.stdout
    before_control=control()
    access=query("""SELECT IS_SRVROLEMEMBER('sysadmin') AS sysadmin,IS_MEMBER('db_owner') AS db_owner,
        HAS_PERMS_BY_NAME('cdc.Sales_Orders_CT','OBJECT','SELECT') AS can_read_cdc,
        HAS_PERMS_BY_NAME('cdc.Sales_Orders_CT','OBJECT','DELETE') AS can_delete_cdc,
        HAS_PERMS_BY_NAME('Sales.Orders','OBJECT','UPDATE') AS can_update_source,
        HAS_PERMS_BY_NAME('sys.sp_cdc_cleanup_change_table','OBJECT','EXECUTE') AS can_execute_cleanup
        FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;""")
    assert access=={'sysadmin':0,'db_owner':0,'can_read_cdc':1,'can_delete_cdc':0,'can_update_source':0,'can_execute_cleanup':0}, 'SQL audit privileges differ from their contract'
    before=collect(policy)
    resolution_spec=importlib.util.spec_from_file_location('resolution_input',SERVICE/'scripts/resolution-input.py')
    resolution_module=importlib.util.module_from_spec(resolution_spec);resolution_spec.loader.exec_module(resolution_module)
    resolution_before=resolution_module.collect_resolution()
    checkpoint_spec=importlib.util.spec_from_file_location('checkpoint_input',SERVICE/'scripts/checkpoint-input.py')
    checkpoint_module=importlib.util.module_from_spec(checkpoint_spec);checkpoint_spec.loader.exec_module(checkpoint_module)
    def checkpoint_observation():
        try: return checkpoint_module.collect_checkpoint()
        except (ValueError,FileNotFoundError):
            return {'contract':'cdc-checkpoint-audit-input/v1','current':None,'exercise':None,
                    'reason':'checkpoint_format_state_or_exercise_evidence_unavailable'}
    checkpoint_before=checkpoint_observation()
    save(directory/'input/source.json',before,10010)
    save(directory/'input/policy.json',policy,10010)
    save(directory/'input/resolution.json',resolution_before,10010)
    save(directory/'input/checkpoint.json',checkpoint_before,10010)
    print(json.dumps({'run_id':run_id,'stage':'source_observed','retained_row_images':sum(map(len,before['tables'].values()))}),flush=True)
    tests=json.loads(job('python','run_tests.py').strip())
    permissions=json.loads(job('python','check_access.py').strip())
    print(json.dumps({'run_id':run_id,'stage':'read_only_permissions_verified'}),flush=True)
    job()
    after=collect(policy,before['upper_lsn'])
    resolution_after=resolution_module.collect_resolution()
    checkpoint_after=checkpoint_observation()
    after_control=control()
    report=json.loads((directory/'output/audit.json').read_text())
    stable=fingerprint(before)==fingerprint(after)
    def cleanup_disabled(control):
        jobs=control.get('cleanup_jobs') or []
        return len(jobs)==1 and jobs[0]['enabled']==0
    report['checks']['SOURCE_OBSERVATION_STABLE']=stable
    report['checks']['WAREHOUSE_RESOLUTION_OBSERVATION_STABLE']=resolution_before==resolution_after
    report['checks']['CONNECTOR_OBSERVATION_STABLE']=bool(checkpoint_before.get('current')) and (
        checkpoint_before.get('current')==checkpoint_after.get('current') and
        checkpoint_before.get('exercise_sha256')==checkpoint_after.get('exercise_sha256'))
    report['checks']['AUTOMATIC_CLEANUP_DISABLED']=cleanup_disabled(before_control) and cleanup_disabled(after_control)
    report['decision']=decision(report['checks'])
    report.update(run_id=run_id,completed_at=datetime.now(timezone.utc).isoformat(),
        sql_access_checks=access,storage_access_checks=permissions,unit_tests=tests,unit_tests_passed=tests['tests_run'],
        source_fingerprint_before=fingerprint(before),source_fingerprint_after=fingerprint(after),
        cleanup_job_before=before_control,cleanup_job_after=after_control,
        image_id=subprocess.check_output(['docker','image','inspect','--format','{{.Id}}','decom-cdc-purge-audit:3'],text=True).strip())
    health={}
    names=subprocess.check_output(['docker','ps','--format','{{.Names}}'],text=True).splitlines()
    for name in sorted(n for n in names if n.startswith('decom-')):
        state=json.loads(subprocess.check_output(['docker','inspect','--format','{{json .State}}',name],text=True))
        health[name]=state.get('Health',{}).get('Status',state['Status'])
    report['service_health']=health
    report['checks']['SERVICES_HEALTHY_AT_COMPLETION']=len(health)>=13 and all(value=='healthy' for value in health.values())
    report['decision']=decision(report['checks'])
    report['provenance_sha256']={str(p.relative_to(SERVICE)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted([*SERVICE.glob('app/*.py'),*SERVICE.glob('scripts/*.py'),*SERVICE.glob('tests/*.py'),
                         *SERVICE.glob('tests/fixtures/*'),SERVICE/'Dockerfile',SERVICE/'compose.yaml',SERVICE/'policy.json'])
        if p.is_file()}
    dependencies=['cdc/archiver/archive.py','warehouse/loader/load.py','warehouse/loader/sales_contract.py',
                  'warehouse/loader/resolve.py','warehouse/loader/resolve_core.py','warehouse/loader/requirements.txt']
    report['dependency_provenance_sha256']={name:hashlib.sha256((SERVICE.parent/name).read_bytes()).hexdigest()
                                          for name in dependencies}
    save(directory/'input/source-after.json',after)
    save(directory/'input/resolution-after.json',resolution_after)
    save(directory/'input/checkpoint-after.json',checkpoint_after)
    save(directory/'report.json',report)
    digest=hashlib.sha256((directory/'report.json').read_bytes()).hexdigest()
    save(ROOT/'purge/latest.json',{'run_id':run_id,'report':str(directory/'report.json'),'sha256':digest})
    destination=ROOT/'validation/cdc-purge-audit-v3-latest.json'
    save(destination,report)
    print(json.dumps({'evidence':str(destination),'run_id':run_id,'purge_authorized':False,
                      'verified_manifests':report['archive']['verified_manifests'],
                      'identity_coverage':report['retained_identity_coverage'],
                      'value_images_matched':report['captured_column_values']['matching_images'],
                      'compared_cells':report['captured_column_values']['compared_cells'],
                      'blockers':report['decision']['blockers']}),flush=True)


if __name__=='__main__':
    with (ROOT/'purge/audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main()
