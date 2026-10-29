"""Manual VM-only lab.CdcProbe CDC cleanup. --execute is the explicit delete switch.

Even without --execute, this emits one isolated probe and briefly stops/restarts
Debezium to preserve a flushed checkpoint. Business tables are never modified.
"""
import argparse
from collections import Counter
from datetime import datetime,timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT=Path('/srv/decom-sqlserver')
HERE=Path(__file__).resolve().parent
CDC=ROOT/'workspace/services/cdc'
IMAGE='decom-cdc-purge-audit:validated-v3-20261029'
IMAGE_ID='sha256:136eb594a583a89240ef14b8833cd53bc88c292d6e4be0c27d0db8a22418b72b'
sys.path.insert(0,str(HERE));sys.path.insert(0,str(HERE.parent/'app'))
from source import collect
from pilot_core import plan,execution_sql,observation_sql,METADATA_SQL,verify_after,TABLE,lsn,digest
from offset_checkpoint import position
from resume_audit import SourcePositions


def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


checkpoint=module('checkpoint_input',HERE/'checkpoint-input.py')
admin=module('source_admin',ROOT/'workspace/services/warehouse/scripts/source-export.py')


def save(path,value,uid=None):
    temporary=path.with_name(path.name+'.tmp')
    with temporary.open('wb') as stream:
        stream.write(value if isinstance(value,bytes) else (json.dumps(value,indent=2)+'\n').encode())
        stream.flush();os.fsync(stream.fileno())
    temporary.chmod(0o400)
    if uid is not None: os.chown(temporary,uid,uid)
    os.replace(temporary,path)
    fd=os.open(path.parent,os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def probe_positions(verification,identity,source,policy):
    binding=SourcePositions(source,policy,True);events={}
    for event in verification['probes'][identity]:
        row={'ProbeID':identity,'Payload':event['value']}
        envelope={'op':event['op'],'before':row,'after':row,
                  'source':{**event,'schema':'lab','table':'CdcProbe'}}
        point=binding.for_event(envelope)
        if event['op'] in events and events[event['op']]!=point:
            raise ValueError('Probe redelivered at a different source position')
        events[event['op']]=point
    if set(events)!={'c','u','d'} or not events['c']<events['u']<events['d']:
        raise ValueError('Probe source order differs')
    return events


def main(execute):
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the provisioned VM')
    os.umask(0o077)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:12]
    base=ROOT/'purge/pilots';base.mkdir(mode=0o700,exist_ok=True)
    directory=base/run_id;directory.mkdir(mode=0o700)
    incoming=directory/'input';incoming.mkdir(mode=0o700);os.chown(incoming,10003,10003)
    attempted=False;execution=None;approved=None;before=None
    def stage(name,**details): print(json.dumps({'run_id':run_id,'stage':name,**details}),flush=True)
    def run(command,timeout=180,cwd=CDC):
        result=subprocess.run(command,cwd=cwd,text=True,capture_output=True,timeout=timeout)
        log=directory/('command-'+uuid.uuid4().hex[:8]+'.log')
        save(log,(result.stdout+result.stderr).encode())
        if result.returncode: raise RuntimeError('Pilot command failed; protected log: '+str(log))
        return result.stdout
    def compose(*args): return run(['docker','compose',*args],timeout=240)
    def probe():
        return json.loads(run(['python3',str(CDC/'scripts/probe-host.py')]).strip().splitlines()[-1])['probe_id']
    def wait_probe(identity):
        for _ in range(45):
            try: return json.loads(compose('exec','-T','archiver','python','verify_archive.py','--probe',identity))
            except RuntimeError: time.sleep(2)
        raise RuntimeError('Probe did not reach its checksum-verified archive')
    def evidence(label):
        output=directory/label;output.mkdir(mode=0o700);os.chown(output,10003,10003)
        run(['docker','run','--rm','--name','decom-purge-pilot-'+run_id+'-'+label,
             '--user','10003:10003','--network','decom-cdc-archive','--read-only','--memory','512m','--cpus','1',
             '--cap-drop','ALL','--security-opt','no-new-privileges',
             '-v',str(HERE.parent/'app')+':/pilot:ro','-v',str(incoming)+':/input:ro',
             '-v',str(output)+':/output','-v',str(ROOT/'secrets/cdc/s3-archive.json')+':/run/secrets/archive:ro',
             '-e','S3_CREDENTIALS_FILE=/run/secrets/archive','-e','S3_ENDPOINT=http://seaweedfs:8333',
             IMAGE,'python','/pilot/pilot_archive.py'],timeout=240)
        return json.loads((output/'archive-proof.json').read_text())
    def healthy(names):
        result={name:checkpoint.inspect(name)['state'].get('Health',{}).get('Status') for name in names}
        if any(value!='healthy' for value in result.values()): raise ValueError('A permanent service is not healthy')
        return result
    def state_unchanged(left,right):
        for key in ['container_id','image_id','config_sha256','state_directory','source_container_id']:
            if left[key]!=right[key]: raise ValueError('Connector/source environment changed')
        if left['files']['schema-history.dat']!=right['files']['schema-history.dat']:
            raise ValueError('Schema history changed')
        if right['checkpoint']['command_id'] is None: raise ValueError('Pilot expects the validated direct query mode')
    try:
        stage('preflight',execute=execute,scope=TABLE)
        actual=run(['docker','image','inspect','--format','{{.Id}}',IMAGE]).strip()
        if actual!=IMAGE_ID: raise ValueError('Validated archive verifier image changed')
        names=sorted(n for n in run(['docker','ps','--format','{{.Names}}']).splitlines() if n.startswith('decom-'))
        if len(names)!=13: raise ValueError('Expected the existing thirteen healthy permanent services')
        healthy(names)
        initial,_=checkpoint.stable_state()
        if initial['checkpoint']['command_id'] is None: raise ValueError('Unexpected connector query mode')
        policy=json.loads((HERE.parent/'policy.json').read_text())
        anchor=probe();anchor_archive=wait_probe(anchor)
        stage('anchor_archived',probe_id=anchor)
        try:
            compose('stop','-t','60','debezium')
            stopped,state_files=checkpoint.state(flush=True);state_unchanged(initial,stopped)
            for name,data in state_files.items(): save(directory/name,data)
            save(directory/'checkpoint-before.json',stopped)
            before=collect(policy);save(directory/'source-before.json',before)
            metadata=admin.query(observation_sql()+METADATA_SQL);save(directory/'metadata-before.json',metadata)
            points=probe_positions(anchor_archive,anchor,before,policy)
            if position(stopped['checkpoint'])!=points['d']:
                raise ValueError('Persisted checkpoint must equal the verified anchor delete')
            approved=plan(before,metadata,stopped,anchor)
            save(incoming/'probe-source.json',{'observation_format':'sqlserver-cdc-probe-images/v1',
                'upper_lsn':before['upper_lsn'],'captured_columns':{TABLE:before['captured_columns'][TABLE]},
                'tables':{TABLE:before['tables'][TABLE]}},uid=10003)
            save(incoming/'policy.json',policy,uid=10003);save(incoming/'run.json',{'run_id':run_id},uid=10003)
            proof=evidence('archive-before');save(directory/'plan.json',approved)
            stage('dry_run_passed',candidate_images=approved['candidate_count'],
                  candidate_cycles=approved['candidate_cycles'],retained_images=approved['retained_rows'],
                  effective_lsn=approved['effective_lsn'],archive_backup=proof['source_image_backup'])
            if execute:
                fresh=collect(policy,upper=before['upper_lsn'],columns=before['captured_columns'])
                for key in ['tables','capture_instances','captured_columns','upper_lsn']:
                    if fresh[key]!=before[key]: raise ValueError('Source changed after archival verification')
                fresh_meta=admin.query(observation_sql(approved['effective_lsn'])+METADATA_SQL)
                if plan(fresh,fresh_meta,stopped,anchor)!=approved: raise ValueError('Plan changed before execution')
                fresh_checkpoint,_=checkpoint.state(flush=True)
                if fresh_checkpoint!=stopped: raise ValueError('Stopped connector state changed')
                if checkpoint.inspect('decom-cdc-debezium-1')['state']['Running']:
                    raise ValueError('Connector was restarted by another operator')
                save(directory/'execution-intent.json',{'run_id':run_id,'plan_sha256':digest(approved),
                    'procedure':'sys.sp_cdc_cleanup_change_table','scope':TABLE,'at':datetime.now(timezone.utc).isoformat()})
                attempted=True
                # A cleanup error is never retried: the native procedure may have
                # advanced its watermark even if physical deletion then failed.
                execution=admin.query(execution_sql(approved));save(directory/'execution-result.json',execution)
                stage('cleanup_returned',**execution)
                after=collect(policy,upper=before['upper_lsn'],columns=before['captured_columns'])
                save(directory/'source-after.json',after)
                reconciliation=verify_after(before,after,approved)
                if (execution['return_code']!=0 or execution['cleanup_failed'] or execution['candidates_remaining']!=0
                        or lsn(execution['actual_low_watermark'])!=approved['effective_lsn']):
                    raise ValueError('Native cleanup did not complete as planned')
                readback=evidence('archive-after')
                if readback!=proof: raise ValueError('Archive or evidence changed after cleanup')
        finally:
            # Always restore capture with the same container and persistent state.
            compose('up','-d','--no-deps','--wait','--wait-timeout','180','debezium')
        if not execute:
            save(directory/'dry-run.json',{'run_id':run_id,'plan':approved,'archive':proof,'changes_deleted':0})
            stage('dry_run_complete',report=str(directory/'dry-run.json'));return
        stage('connector_resumed')
        after_id=probe();verification=wait_probe(after_id)
        final_source=collect(policy);save(directory/'source-final.json',final_source)
        final_positions=probe_positions(verification,after_id,final_source,policy)
        for _ in range(30):
            current,state_files=checkpoint.stable_state()
            if position(current['checkpoint'])==final_positions['d']: break
            time.sleep(1)
        else: raise ValueError('Persisted checkpoint did not reach the post-purge probe')
        state_unchanged(stopped,current)
        if position(current['checkpoint'])<=position(stopped['checkpoint']): raise ValueError('Checkpoint did not advance')
        save(directory/'checkpoint-after.json',current)
        for name,data in state_files.items(): save(directory/('after-'+name),data)
        final_meta=admin.query(observation_sql(approved['effective_lsn'])+METADATA_SQL)
        save(directory/'metadata-final.json',final_meta)
        if final_meta['live_probe_rows']!=0 or final_meta['candidate_count']!=0 or final_meta['cleanup_jobs']!=metadata['cleanup_jobs']:
            raise ValueError('Unexpected final source/cleanup configuration')
        if len(final_source['tables'][TABLE])!=approved['retained_rows']+4:
            raise ValueError('Unexpected final technical image count')
        service_health=healthy(names)
        report={'contract':'cdc-probe-purge-pilot/v1','run_id':run_id,'passed':True,
            'completed_at':datetime.now(timezone.utc).isoformat(),'mode':'manual-technical-pilot',
            'plan':approved,'archive':proof,'execution':execution,'reconciliation':reconciliation,
            'changes_deleted':reconciliation['exact_planned_images_removed'],
            'final_probe_cdc_images':len(final_source['tables'][TABLE]),'live_probe_rows':0,
            'continuity':{'anchor_id':anchor,'post_purge_probe_id':after_id,'post_purge_events':['c','u','d'],
                'checkpoint_before':stopped,'checkpoint_after':current,'checkpoint_advanced':True,
                'same_container_image_config_history_and_mount':True,'state_fsynced_before_cleanup':True,
                'archive_and_backup_readback_after_cleanup':True},
            'native_cleanup_jobs':final_meta['cleanup_jobs'],'services':service_health,
            'scope_exception':{'retention_minutes':15,'pre_bootstrap_strategy':'Verified direct SQL CDC image backup',
                'off_vm_recovery_tested':False,'crash_durability_certified':False,
                'production_transaction_fence_certified':False,'business_purge_authorized':False,
                'automatic_purge_enabled':False,'archive_only_technical_consumer':True},
            'provenance_sha256':{str(p.relative_to(HERE.parent)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(HERE.parent.rglob('*.py'))}}
        save(directory/'report.json',report)
        pointer={'report':str(directory/'report.json'),'sha256':hashlib.sha256((directory/'report.json').read_bytes()).hexdigest()}
        save(ROOT/'purge/pilot-v1-latest.json',pointer)
        stage('passed',changes_deleted=report['changes_deleted'],**pointer)
    except Exception as error:
        failure={'run_id':run_id,'passed':False,'cleanup_attempted':attempted,'execution_result':execution,
                 'error_type':type(error).__name__,'error':str(error),'automatic_retry_allowed':False}
        if attempted and approved:
            try:
                actual=admin.query(observation_sql(approved['effective_lsn'])+METADATA_SQL)
                save(directory/'metadata-on-failure.json',actual)
                failure['observed_low_watermark']=actual['capture']['start_lsn']
                failure['observed_candidates_remaining']=actual['candidate_count']
            except Exception as inspection: failure['inspection_error_type']=type(inspection).__name__
        save(directory/'failure.json',failure)
        stage('failed',cleanup_attempted=attempted,evidence_directory=str(directory));raise


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true',help='Execute the bounded, verified technical cleanup once')
    args=parser.parse_args()
    with (ROOT/'purge/audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main(args.execute)
