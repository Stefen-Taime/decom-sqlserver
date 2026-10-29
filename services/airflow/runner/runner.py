#!/usr/bin/python3
"""Root-owned forced SSH command: bounded operations, never an arbitrary shell or Docker API."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime,timezone

ROOT=Path('/srv/decom-sqlserver')
STATE=ROOT/'airflow/runner'
SERVICE=ROOT/'workspace/services/export'
DATASETS={'invoice_details','order_details'}


def require(condition,message):
    if not condition: raise ValueError(message)


def now(): return datetime.now(timezone.utc).isoformat()


def command(args,timeout=30,check=True):
    result=subprocess.run(args,cwd=SERVICE,stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=timeout)
    if check and result.returncode:
        raise RuntimeError('Operator command failed: '+args[0])
    return result


def atomic(path,value):
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as f:
        temp=Path(f.name);os.chmod(temp,0o600)
        f.write((json.dumps(value,sort_keys=True)+'\n').encode());f.flush();os.fsync(f.fileno())
    os.replace(temp,path)
    fd=os.open(path.parent,os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def validate(request):
    require(isinstance(request,dict),'Request must be an object')
    action=request.get('action')
    require(action in {'preflight','submit','status','invoice_receipt'},'Unsupported action')
    require(request.get('dataset') in DATASETS,'Unsupported dataset')
    fields={'action','dataset'}
    if action!='preflight':
        fields.add('request_id')
        require(isinstance(request.get('request_id'),str) and re.fullmatch(r'af1-[0-9a-f]{40}',request['request_id']),
                'Invalid request identity')
    if action in {'submit','status'}:
        fields.add('operation');require(request.get('operation') in {'export','verify'},'Unsupported operation')
    if action=='submit':
        fields.add('attempt')
        require(type(request.get('attempt')) is int and 1<=request['attempt']<=20,'Invalid task attempt')
    require(set(request)==fields,'Unexpected request fields')
    if action=='invoice_receipt': require(request['dataset']=='invoice_details','No RabbitMQ receipt for this dataset')
    return request


def preflight(dataset):
    containers=['decom-sqlserver-sqlserver-1','decom-warehouse-postgres-1','decom-warehouse-loader-1','decom-cdc-seaweedfs-1',
                'decom-cdc-debezium-1','decom-cdc-archiver-1']
    if dataset=='invoice_details':
        containers+=['decom-messaging-rabbitmq-1','decom-messaging-dispatcher-1','decom-messaging-invoice-app-1']
    for name in containers:
        state=json.loads(command(['docker','inspect','--format','{{json .State}}',name]).stdout)
        require(state['Running'] and state.get('Health',{}).get('Status')=='healthy','Dependency unhealthy: '+name)
    query="SELECT json_build_object('consumer',consumer,'last_success_at',last_success_at,'error_type',error_type,'recent',last_success_at>now()-interval '180 seconds') FROM ops.consumer_status WHERE consumer='sales/v2'"
    result=command(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-U','warehouse_admin','-d','warehouse','-c',query])
    value=json.loads(result.stdout)
    require(value['recent'] and value['error_type'] is None,'Warehouse ingestion is unhealthy or its scan is stale')
    return {'status':'ready','dataset':dataset,'ingestion':value,'checked_at':now(),
            'source_transaction_boundary_certified':False,'purge_authorized':False}


def state_file(request): return STATE/'requests'/(request['request_id']+'.json')


def load(request):
    path=state_file(request)
    state=json.loads(path.read_text()) if path.exists() else {
        'request_id':request['request_id'],'dataset':request['dataset'],'created_at':now(),'jobs':{}}
    require(state['dataset']==request['dataset'],'Request dataset changed')
    return path,state


def inspect_job(job):
    result=command(['docker','inspect',job['container']],check=False)
    if result.returncode: return None
    value=json.loads(result.stdout)[0]
    labels=value['Config']['Labels'] or {}
    require(labels.get('decom.airflow.request')==job['request_id'] and
            labels.get('decom.airflow.operation')==job['operation'],'Container ownership mismatch')
    return value


def collect(path,state,operation):
    job=state['jobs'][operation]
    if job['status'] in {'succeeded','failed'}: return job
    container=inspect_job(job)
    if container is None:
        return job
    status=container['State']
    if status['Running'] or status['Status']=='created': return job
    logs=command(['docker','logs',job['container']],check=False)
    log_path=STATE/'logs'/(job['container']+'.log')
    log_path.write_text(logs.stdout+logs.stderr);log_path.chmod(0o600)
    job.update(finished_at=now(),exit_code=status['ExitCode'],oom_killed=status.get('OOMKilled',False))
    if status['ExitCode']==0:
        try:
            result=json.loads(logs.stdout.strip().splitlines()[-1])
            if operation=='export':
                require(result['status']=='published' and result['dataset']==state['dataset']
                        and result['request_id']==state['request_id'],'Export result identity mismatch')
                result={k:result[k] for k in ['dataset','request_id','run_id','publication','attempts']}
            else:
                expected=state['jobs']['export']['result']
                require(result['status']=='validated' and result['dataset']==state['dataset']
                        and result['publication']==expected['publication'],'Consumer result identity mismatch')
                result={**expected,'rows':result['statistics']['rows'],'row_sha256':result['statistics']['row_sha256'],
                        'manifest_sha256':result['manifest_sha256'],'files':result['files']}
            job.update(status='succeeded',result=result)
        except (ValueError,KeyError,IndexError,TypeError):
            job.update(status='failed',error='Invalid job completion record')
    else:
        job.update(status='failed',error='Job failed; inspect protected operator log')
    atomic(path,state)
    command(['docker','rm',job['container']])
    return job


def public(job):
    result={k:job[k] for k in ['status','attempt','container']}
    if 'result' in job: result['result']=job['result']
    if 'error' in job: result['error']=job['error']
    return result


def job_request(request):
    path,state=load(request);operation=request['operation']
    if operation in state['jobs']:
        job=collect(path,state,operation)
        if job['status']=='succeeded': return public(job)
        if request['action']=='status': return public(job)
        if job['status']=='failed' and request['attempt']<=job['attempt']: return public(job)
        if job['status']=='running':
            container=inspect_job(job)
            if container is not None:
                if container['State']['Status']=='created':
                    # A lost submit process may have created the container without starting it.
                    active=command(['docker','ps','--filter','label=decom.airflow.managed=true','--format','{{.Names}}']).stdout.strip()
                    if active: return {'status':'busy'}
                    command(['docker','start',job['container']])
                return public(job)
    elif request['action']=='status':
        return {'status':'not_submitted'}
    if operation=='verify':
        require('export' in state['jobs'] and collect(path,state,'export')['status']=='succeeded','Export is not complete')
    # Bound resources even if the orchestration key is misused: one task container at a time.
    active=command(['docker','ps','--filter','label=decom.airflow.managed=true','--format','{{.Names}}']).stdout.strip()
    if active: return {'status':'busy'}
    job={'status':'running','request_id':request['request_id'],'operation':operation,'attempt':request['attempt'],
         'container':f"decom-af-{operation}-{request['request_id'][4:]}-a{request['attempt']}", 'started_at':now()}
    state['jobs'][operation]=job;atomic(path,state)
    # A saved running state with an absent container is safe to submit again after a host-script interruption.
    args=['docker','compose','run','-d','--no-deps','--name',job['container'],
          '--label','decom.airflow.managed=true','--label','decom.airflow.request='+request['request_id'],
          '--label','decom.airflow.operation='+operation]
    if operation=='export':
        args+=['exporter','export.py','--dataset',request['dataset'],'--request-id',request['request_id']]
    else:
        args+=['consumer','consumer.py','read','--dataset',request['dataset'],
               '--publication',state['jobs']['export']['result']['publication']]
    command(args,timeout=60)
    return public(job)


def invoice_receipt(request):
    path,state=load(request)
    require('verify' in state['jobs'] and collect(path,state,'verify')['status']=='succeeded','Validated export required')
    expected=state['jobs']['verify']['result']
    database=ROOT/'messaging/app/invoices.sqlite3'
    with sqlite3.connect('file:'+str(database)+'?mode=ro',uri=True,timeout=5) as db:
        db.row_factory=sqlite3.Row
        row=db.execute('SELECT event_id,run_id,body,statistics,decision,committed_at FROM deliveries WHERE publication=?',
                       (expected['publication'],)).fetchone()
        if not row: return {'processed':False,'run_id':expected['run_id']}
        body=json.loads(row['body']);stats=json.loads(row['statistics'])
        require(row['run_id']==expected['run_id'] and body['manifest_sha256']==expected['manifest_sha256']
                and stats['row_sha256']==expected['row_sha256'] and stats['rows']==expected['rows'],'Application receipt mismatch')
        return {'processed':True,'event_id':row['event_id'],'run_id':row['run_id'],
                'decision':row['decision'],'rows':stats['rows'],'committed_at':row['committed_at'],
                'purge_authorized':False}


def main(request):
    request=validate(request)
    if request['action']=='preflight': return preflight(request['dataset'])
    with (STATE/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        if request['action']=='invoice_receipt': return invoice_receipt(request)
        return job_request(request)


if __name__=='__main__':
    try:
        def timed_out(signum, frame):
            raise TimeoutError('Runner request deadline exceeded')
        signal.signal(signal.SIGALRM,timed_out)
        signal.alarm(30)
        raw=sys.stdin.buffer.read(4097)
        require(len(raw)<=4096,'Request too large')
        signal.alarm(120)
        result=main(json.loads(raw))
        print(json.dumps(result))
    except Exception as exc:
        # No credentials, arbitrary Docker output or business rows are returned to SSH clients.
        print(json.dumps({'status':'error','error_type':type(exc).__name__,
                          'message':str(exc) if isinstance(exc,ValueError) else 'Operator investigation required'}))
        sys.exit(1)
