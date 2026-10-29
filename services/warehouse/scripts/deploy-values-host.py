"""Validate in isolated databases, back up the warehouse, migrate, replay and certify values."""
from datetime import datetime,timezone
import hashlib
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

ROOT=Path('/srv/decom-sqlserver');SERVICE=ROOT/'workspace/services/warehouse'


def command(args,log):
    with log.open('w') as stream:
        result=subprocess.run(args,cwd=SERVICE,text=True,stdout=stream,stderr=subprocess.STDOUT)
    log.chmod(0o600)
    if result.returncode: raise RuntimeError('Command failed; protected diagnostics: '+str(log))
    return log.read_text()


def sql(query,user='warehouse_admin'):
    return subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-q','-A','-t',
        '-v','ON_ERROR_STOP=1','-U',user,'-d','warehouse','-c',query],text=True).strip()


def journal_hash():
    query='COPY (SELECT event_id,semantic_sha256,hashdiff FROM staging.cdc_event ORDER BY event_id) TO STDOUT'
    process=subprocess.Popen(['docker','exec','decom-warehouse-postgres-1','psql','-X','-q','-U','warehouse_admin','-d','warehouse','-c',query],stdout=subprocess.PIPE)
    checksum=hashlib.sha256()
    while chunk:=process.stdout.read(65536): checksum.update(chunk)
    if process.wait(): raise RuntimeError('Could not hash raw journal')
    return checksum.hexdigest()


def counts():
    return json.loads(sql("SELECT json_build_object('events',(SELECT count(*) FROM staging.cdc_event),"
        "'sales_batches',(SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'),"
        "'customers',(SELECT count(*) FROM mart.customers),'orders',(SELECT count(*) FROM mart.orders),"
        "'order_lines',(SELECT count(*) FROM mart.order_lines),'invoices',(SELECT count(*) FROM mart.invoices),"
        "'invoice_lines',(SELECT count(*) FROM mart.invoice_lines))"))


def main():
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the VM')
    os.umask(0o077)
    release=ROOT/'releases/cdc-values-v1-20261029';release.mkdir(exist_ok=True,mode=0o700)
    run=release/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]);run.mkdir(mode=0o700)
    tests={}
    for name,script in [('warehouse','test-host.py'),('sql_object','test-order-updates-host.py')]:
        output=command(['python3','scripts/'+script],run/(name+'-tests.log'))
        match=re.search(r'(\d+) passed',output)
        if not match: raise RuntimeError('Missing test completion evidence')
        tests[name]=int(match.group(1));print(json.dumps({'stage':'tests_passed','suite':name,'count':tests[name]}),flush=True)
    prior_image=subprocess.check_output(['docker','inspect','--format','{{.Image}}','decom-warehouse-loader-1'],text=True).strip()
    original=release/'previous-loader-image.txt'
    if not original.exists():
        original.write_text(prior_image+'\n');original.chmod(0o400)
        subprocess.run(['docker','tag',prior_image,'decom-warehouse-loader:before-cdc-values-20261029'],check=True)
    subprocess.run(['docker','compose','stop','loader'],cwd=SERVICE,check=True)
    migrated=False
    try:
        before=counts();raw_before=journal_hash()
        backup=release/'warehouse-before-values.dump'
        if not backup.exists():
            temporary=run/'warehouse-backup.partial'
            with temporary.open('xb') as stream:
                subprocess.run(['docker','exec','decom-warehouse-postgres-1','pg_dump','-U','warehouse_admin','-d','warehouse','-Fc'],stdout=stream,check=True)
                stream.flush();os.fsync(stream.fileno())
            temporary.chmod(0o400);os.replace(temporary,backup)
        with backup.open('rb') as stream: backup_hash=hashlib.file_digest(stream,'sha256').hexdigest()
        print(json.dumps({'stage':'backup_complete','bytes':backup.stat().st_size}),flush=True)
        command(['python3','scripts/migrate-host.py'],run/'migration.log');migrated=True
        replay=command(['docker','compose','run','--rm','--no-deps','backfill','python','load.py','--replay'],run/'archive-replay.log')
        print(json.dumps({'stage':'archive_replay_complete'}),flush=True)
        command(['python3','scripts/certify-values-host.py'],run/'certification.log')
        proof=json.loads((ROOT/'validation/cdc-value-reconstruction-v1-latest.json').read_text())
        output=command(['docker','compose','run','--rm','--no-deps','backfill','python','verify_values.py'],run/'affected-replay.log')
        replay_result=json.loads(next(line for line in output.splitlines() if line.startswith('{')))
        after=counts();raw_after=journal_hash()
        if before!=after or raw_before!=raw_after: raise RuntimeError('Raw journal or current row counts changed unexpectedly')
        exports=json.loads(sql("SELECT json_build_object('order_rows',(SELECT count(*) FROM delivery.order_details_v1),"
            "'invoice_rows',(SELECT count(*) FROM delivery.invoice_details_v1),"
            "'guard_executable',has_function_privilege(current_user,'business.assert_ready(text)','EXECUTE'))",'warehouse_exporter'))
        report={'contract':'cdc-value-reconstruction-deployment/v1','completed_at':datetime.now(timezone.utc).isoformat(),
            'tests':tests,'before':before,'after':after,'raw_journal_sha256_before':raw_before,'raw_journal_sha256_after':raw_after,
            'source_validation':proof,'affected_archive_replay':replay_result,'export_reader_checks':exports,
            'backup':{'path':str(backup),'sha256':backup_hash,'bytes':backup.stat().st_size,'off_vm_restore_tested':False},
            'purge_authorized':False,'source_rows_deleted':0,'raw_archives_rewritten':0,
            'previous_loader_image':original.read_text().strip(),
            'loader_image':subprocess.check_output(['docker','image','inspect','--format','{{.Id}}','decom-warehouse-loader:3'],text=True).strip(),
            'provenance_sha256':{str(p.relative_to(SERVICE)):hashlib.sha256(p.read_bytes()).hexdigest()
                for folder in ['loader','scripts','tests','sql'] for p in sorted((SERVICE/folder).glob('*')) if p.suffix in {'.py','.sql'}}}
        path=run/'report.json';path.write_text(json.dumps(report,indent=2)+'\n');path.chmod(0o400)
        destination=ROOT/'validation/cdc-value-reconstruction-v1-2026-10-29.json'
        destination.write_bytes(path.read_bytes());destination.chmod(0o400)
        print(json.dumps({'stage':'validated','report':str(destination),'tests':tests,'derived_images_matching':proof['derived_images_matching']}),flush=True)
    finally:
        # After an applied schema, start the compatible image even if certification was
        # blocked: raw ingestion remains active and unresolved business reads fail closed.
        if migrated:
            subprocess.run(['docker','compose','up','-d','--wait','--wait-timeout','360','loader'],cwd=SERVICE,check=True)
        else:
            subprocess.run(['docker','start','decom-warehouse-loader-1'],check=True)


if __name__=='__main__':
    (ROOT/'releases/cdc-values-v1-20261029').mkdir(parents=True,exist_ok=True,mode=0o700)
    with (ROOT/'releases/cdc-values-v1-20261029/deploy.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main()
