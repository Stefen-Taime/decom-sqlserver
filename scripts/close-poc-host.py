"""Reproduce the POC closure on its existing VM; temporarily stop SQL Server.

Runs two normal Airflow DAGs, captures fresh source procedure reference results,
then verifies the migrated procedure and consumers with SQL Server stopped.
Always attempts to restore SQL Server and Debezium. No deployment or purge.
"""
from datetime import datetime,timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time
import uuid

ROOT=Path('/srv/decom-sqlserver')
WORK=ROOT/'workspace'
sys.path.insert(0,str(WORK/'services/airflow/scripts'))
sys.path.insert(0,str(WORK/'services/airflow/dags'))
sys.path.insert(0,str(WORK/'services/purge/app'))
from api import Client
from pipeline_client import request_id
from offset_checkpoint import position
from audit_core import lsn


def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


checkpoint=module('checkpoint_input',WORK/'services/purge/scripts/checkpoint-input.py')
source=module('source_export',WORK/'services/warehouse/scripts/source-export.py')


def now(): return datetime.now(timezone.utc).isoformat()


def save(path,value):
    temporary=path.with_name(path.name+'.tmp')
    with temporary.open('wb') as stream:
        stream.write(value if isinstance(value,bytes) else (json.dumps(value,indent=2)+'\n').encode())
        stream.flush();os.fsync(stream.fileno())
    temporary.chmod(0o400);os.replace(temporary,path)


def main():
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the existing Linux VM')
    os.umask(0o077)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    directory=ROOT/'validation'/('poc-closure-'+run_id);directory.mkdir(mode=0o700)
    report={'contract':'migration-poc-closure/v1','run_id':run_id,'started_at':now(),
            'passed':False,'source_retired':False,'business_purge_authorized':False}
    def stage(label,**data):
        save(directory/'progress.json',report)
        print(json.dumps({'stage':label,'run_id':run_id,**data}),flush=True)
    def command(args,*,cwd=WORK,timeout=600):
        result=subprocess.run(args,cwd=cwd,capture_output=True,text=True,timeout=timeout)
        log=directory/('command-'+uuid.uuid4().hex[:8]+'.log')
        save(log,(result.stdout+result.stderr).encode())
        if result.returncode: raise RuntimeError('Command failed; protected diagnostics: '+str(log))
        return result.stdout
    def compose(service,*args,timeout=600):
        return command(['docker','compose',*args],cwd=WORK/'services'/service,timeout=timeout)
    def last_json(value): return json.loads(value.strip().splitlines()[-1])
    def healthy(names):
        result={}
        for name in names:
            state=checkpoint.inspect(name)['state']
            if not state['Running'] or state.get('Health',{}).get('Status')!='healthy' or state['OOMKilled']:
                raise ValueError('Service is not healthy: '+name)
            result[name]='healthy'
        return result
    def offline():
        for name in ['decom-sqlserver-sqlserver-1','decom-cdc-debezium-1']:
            if checkpoint.inspect(name)['state']['Running']: raise ValueError('Source/capture must remain stopped')
    def app_status():
        # Status must not run the application's journal-mode initialization while
        # its writer is replacing a snapshot. Busy readers simply poll again.
        try:
            with sqlite3.connect('file:'+str(ROOT/'messaging/app/invoices.sqlite3')+'?mode=ro',uri=True,timeout=1) as db:
                db.row_factory=sqlite3.Row
                row=db.execute('SELECT d.* FROM active_snapshot a JOIN deliveries d USING(event_id)').fetchone()
                return {'active':dict(row) if row else {}}
        except sqlite3.OperationalError as error:
            if 'locked' in str(error).lower(): return {'active':{}}
            raise
    try:
        names=sorted(n for n in command(['docker','ps','--format','{{.Names}}']).splitlines() if n.startswith('decom-'))
        if len(names)!=13: raise ValueError('Expected thirteen permanent services')
        report['initial_services']=healthy(names)
        initial,_=checkpoint.stable_state()
        client=Client()
        if client.request('GET','/api/v2/importErrors')['total_entries']: raise ValueError('Airflow import error')
        dag_configs={d['dag_id']:d for d in client.request('GET','/api/v2/dags')['dags']}
        if any(dag_configs[d]['is_paused'] for d in ['export_invoices','export_orders']):
            raise ValueError('Existing export DAG must be enabled; no schedule configuration is changed')
        report['airflow']={}
        for dag,dataset in [('export_invoices','invoice_details'),('export_orders','order_details')]:
            dag_run='poc_closure__'+run_id
            item={'dag_run_id':dag_run,'dataset':dataset,'request_id':request_id(dataset,dag_run)}
            report['airflow'][dag]=item
            save(directory/'progress.json',report)
            client.request('POST','/api/v2/dags/'+dag+'/dagRuns',{
                'dag_run_id':dag_run,'logical_date':None,'conf':{'fail_after_export_once':False},
                'note':'POC closure: normal end-to-end run before the controlled source outage.'})
        stage('airflow_runs_started')
        deadline=time.monotonic()+1200;previous=None
        while time.monotonic()<deadline:
            states={}
            for dag,item in report['airflow'].items():
                base='/api/v2/dags/'+dag+'/dagRuns/'+item['dag_run_id']
                item['state']=client.request('GET',base)['state']
                tasks=client.request('GET',base+'/taskInstances')['task_instances']
                item['tasks']=[{k:t.get(k) for k in ['task_id','state','try_number','start_date','end_date']} for t in tasks]
                states[dag]={'state':item['state'],'tasks':{t['task_id']:t['state'] for t in item['tasks']}}
            if states!=previous: stage('airflow_progress',states=states);previous=states
            if any(i['state']=='failed' for i in report['airflow'].values()): raise ValueError('Closure DAG failed')
            if all(i['state']=='success' for i in report['airflow'].values()): break
            time.sleep(10)
        else: raise TimeoutError('Closure DAG deadline exceeded; retain the same run identities for investigation')
        for item in report['airflow'].values():
            journal=json.loads((ROOT/'airflow/runner/requests'/(item['request_id']+'.json')).read_text())
            item['export']=journal['jobs']['export']['result']
            item['validation']=journal['jobs']['verify']['result']
            expected=228265 if item['dataset']=='invoice_details' else 231412
            if item['validation']['rows']!=expected: raise ValueError('Unexpected export row count')
            if item['dataset']=='invoice_details':
                receipt_command=['/usr/local/libexec/decom-airflow-runner']
                payload={'action':'invoice_receipt','dataset':item['dataset'],'request_id':item['request_id']}
                result=subprocess.run(receipt_command,input=json.dumps(payload),capture_output=True,text=True,timeout=60)
                if result.returncode: raise ValueError('Invoice receipt lookup failed')
                item['receipt']=json.loads(result.stdout)
                if not item['receipt']['processed']: raise ValueError('Invoice receipt missing')
        stage('airflow_passed',rows={'invoice_details':228265,'order_details':231412})
        stage('capturing_fresh_source_procedure_reference')
        reference=directory/'source-procedure'
        report['source_procedure_export']=last_json(command(['python3',str(WORK/'services/warehouse/scripts/export-order-updates-host.py'),
            '--inventory-dir',str(ROOT/'validation/sql-objects-20261028'),'--output-dir',str(reference)],timeout=600))
        report['source_procedure_manifest_sha256']=hashlib.sha256((reference/'manifest.json').read_bytes()).hexdigest()
        stage('source_reference_captured',rows=report['source_procedure_export']['rows'])
        # Start the recovery finally before either stop command: a partial stop
        # must not leave the lab unavailable if the next operation raises.
        try:
            compose('cdc','stop','-t','60','debezium',timeout=90)
            stopped,files=checkpoint.state(flush=True)
            for name,data in files.items(): save(directory/name,data)
            save(directory/'checkpoint-stopped.json',stopped)
            compose('sqlserver','stop','-t','90','sqlserver',timeout=120)
            offline();report['outage_started_at']=now()
            report['offline_services']=healthy([n for n in names if n not in {'decom-sqlserver-sqlserver-1','decom-cdc-debezium-1'}])
            report['source_stopped_state']={k:checkpoint.inspect('decom-sqlserver-sqlserver-1')['state'][k]
                                            for k in ['Running','Status','ExitCode','FinishedAt']}
            stage('source_stopped',downstream_services_healthy=len(report['offline_services']))
            output=compose('warehouse','run','--rm','--no-deps','-T','--user','0:0',
                '-e','PGUSER=warehouse_reader','-e','PGPASSWORD_FILE=/run/secrets/reader',
                '-v',str(ROOT/'secrets/warehouse/reader_password')+':/run/secrets/reader:ro',
                '-v',str(WORK/'services/warehouse/scripts')+':/validation:ro','-v',str(reference)+':/input:ro',
                'backfill','python','/validation/compare-order-updates.py','/input',timeout=600)
            comparison=json.loads(output)
            if not comparison['passed']: raise ValueError('Migrated procedure differs from the fresh source reference')
            report['procedure_without_source']=comparison;offline()
            stage('procedure_without_source_passed',rows=comparison['cases']['full_current']['target_rows'],cases=len(comparison['cases']))
            order_publication=report['airflow']['export_orders']['export']['publication']
            order_read=last_json(compose('export','run','--rm','--no-deps','consumer','consumer.py','read',
                '--dataset','order_details','--publication',order_publication))
            if order_read['status']!='validated' or order_read['statistics']['rows']!=231412:
                raise ValueError('Direct S3 order consumption failed')
            report['direct_s3_without_source']=order_read;offline()
            stage('direct_s3_without_source_passed',rows=order_read['statistics']['rows'])
            exported=last_json(compose('export','run','--rm','--no-deps','exporter','export.py',
                '--dataset','invoice_details','--request-id','poc-close-'+run_id))
            if exported['status']!='published': raise ValueError('Offline warehouse export was not published')
            verified=last_json(compose('export','run','--rm','--no-deps','consumer','consumer.py','read',
                '--dataset','invoice_details','--publication',exported['publication']))
            if verified['status']!='validated' or verified['statistics']['rows']!=228265:
                raise ValueError('Offline invoice export verification failed')
            deadline=time.monotonic()+240
            while time.monotonic()<deadline:
                application=app_status()
                if application.get('active',{}).get('run_id')==exported['run_id']: break
                time.sleep(3)
            else: raise TimeoutError('The invoice application did not receive the new offline export')
            active=application['active'];body=json.loads(active['body']);stats=json.loads(active['statistics'])
            if (active['publication']!=exported['publication'] or body['manifest_sha256']!=verified['manifest_sha256']
                    or stats!=verified['statistics'] or active['decision']!='applied'):
                raise ValueError('Invoice application receipt differs from the offline export')
            audit=json.loads(command(['docker','exec','decom-messaging-invoice-app-1','python','audit_app.py'],timeout=300))
            if not audit['all_stored_rows_reconciled'] or audit['statistics']!=verified['statistics']:
                raise ValueError('Stored application data differs')
            offline()
            report['invoice_without_source']={'export':exported,'validation':verified,
                'receipt':{k:active[k] for k in ['event_id','run_id','publication','decision','committed_at']},
                'application_audit':audit,'automatic_dispatch_and_consumption':True}
            report['offline_checks_completed_at']=now()
            stage('invoice_without_source_passed',rows=audit['statistics']['rows'],new_export_run=exported['run_id'])
        finally:
            stage('restoring_source_and_capture')
            try:
                compose('sqlserver','up','-d','--no-deps','--wait','--wait-timeout','240','sqlserver',timeout=300)
            finally:
                compose('cdc','up','-d','--no-deps','--wait','--wait-timeout','180','debezium',timeout=240)
            report['source_restored_at']=now()
        stage('source_and_capture_restored')
        probe=last_json(command(['python3',str(WORK/'services/cdc/scripts/probe-host.py')]))['probe_id']
        for _ in range(45):
            try:
                archived=json.loads(compose('cdc','exec','-T','archiver','python','verify_archive.py','--probe',probe,timeout=90))
                break
            except RuntimeError: time.sleep(2)
        else: raise ValueError('Fresh CDC probe did not reach its verified archive after recovery')
        delete=next(e for e in archived['probes'][probe] if e['op']=='d')
        for _ in range(30):
            resumed,_=checkpoint.stable_state()
            point=resumed['checkpoint']
            if (point['commit_lsn']==lsn(delete['commit_lsn']) and point['change_lsn']==lsn(delete['change_lsn'])
                    and point['event_serial_no']==delete['event_serial_no']): break
            time.sleep(1)
        else: raise ValueError('Persisted checkpoint did not reach the recovery probe')
        for field in ['container_id','image_id','config_sha256','state_directory','source_container_id']:
            if not initial[field]==stopped[field]==resumed[field]: raise ValueError('Container or persistent-state binding changed')
        if stopped['files']['schema-history.dat']!=resumed['files']['schema-history.dat']:
            raise ValueError('Schema history changed')
        if position(resumed['checkpoint'])<=position(stopped['checkpoint']): raise ValueError('Checkpoint did not advance')
        cleanup=source.query("SELECT j.enabled,c.retention FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
        if cleanup!={'enabled':0,'retention':4320}: raise ValueError('Cleanup configuration changed')
        report['recovery']={'probe_id':probe,'archived_events':archived['probes'][probe],
            'checkpoint_before':stopped['checkpoint'],'checkpoint_after':resumed['checkpoint'],
            'same_containers_images_config_and_history':True,'persisted_checkpoint_advanced':True,
            'native_cleanup_job':cleanup}
        report['final_services']=healthy(names)
        report.update(passed=True,completed_at=now(),poc_status='complete',
            limitations=['POC scope only: five captured business tables and one migrated stored procedure.',
                'Airflow DAGs were validated before the outage; their normal preflight still requires a healthy source.',
                'During the outage, PostgreSQL reads, direct S3 consumption and a new manual invoice export/application delivery passed.',
                'No new source writes can be captured while SQL Server is stopped; this is downstream autonomy on migrated data.',
                'The technical purge pilot remains limited to lab.CdcProbe. No business cleanup or permanent source retirement.',
                'One VM; no production HA, crash-durability or off-VM recovery certification.'],
            provenance_sha256={str(p.relative_to(WORK)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [
                Path(__file__).resolve(),WORK/'services/warehouse/scripts/export-order-updates-host.py',
                WORK/'services/warehouse/scripts/compare-order-updates.py',WORK/'services/rabbitmq/app/audit_app.py']})
        save(directory/'report.json',report)
        pointer={'report':str(directory/'report.json'),'sha256':hashlib.sha256((directory/'report.json').read_bytes()).hexdigest()}
        save(ROOT/'validation/poc-closure-v1-latest.json',pointer)
        stage('poc_complete',**pointer)
    except BaseException as error:
        report.update(passed=False,error_type=type(error).__name__,error=str(error),failed_at=now())
        save(directory/'failure.json',report)
        print(json.dumps({'stage':'failed','evidence_directory':str(directory),'error_type':type(error).__name__}),flush=True)
        raise


if __name__=='__main__':
    def interrupted(signum,frame): raise KeyboardInterrupt('Operator closure interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    with (ROOT/'purge/audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main()
