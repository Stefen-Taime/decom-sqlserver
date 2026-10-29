"""Observe SQL CDC read-only, verify archives, attest unchanged markers, then rebuild derived values."""
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT=Path('/srv/decom-sqlserver');SERVICE=ROOT/'workspace/services/warehouse'
sys.path.insert(0,str(ROOT/'workspace/services/purge/scripts'))
from source import collect


def fingerprint(source):
    return hashlib.sha256(json.dumps({name:source[name] for name in ['upper_lsn','capture_instances','captured_columns','tables']},
                                     sort_keys=True).encode()).hexdigest()


def save(path,value,owner=0):
    path.write_text(json.dumps(value,indent=2)+'\n');os.chown(path,owner,owner);path.chmod(0o400)


def sql(statement):
    result=subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-A','-t',
        '-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],input=statement,text=True,capture_output=True,check=True)
    return result.stdout.strip()


def main():
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the VM')
    os.umask(0o077)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    directory=ROOT/'validation'/('cdc-values-'+run_id);directory.mkdir(mode=0o700)
    for name in ['input','output']:
        path=directory/name;path.mkdir(mode=0o700);os.chown(path,10004,10004)
    policy=json.loads((ROOT/'workspace/services/purge/policy.json').read_text())
    before=collect(policy);save(directory/'input/source.json',before,10004);save(directory/'input/policy.json',policy,10004)
    def job(*args):
        result=subprocess.run(['docker','compose','run','--rm','--no-deps',
            '-v',str(directory/'input')+':/input:ro','-v',str(directory/'output')+':/output',
            'backfill','python',*args],cwd=SERVICE,text=True,capture_output=True)
        log=directory/('command-'+uuid.uuid4().hex[:8]+'.log');log.write_text(result.stdout+result.stderr);log.chmod(0o600)
        if result.returncode: raise RuntimeError('Verification failed; protected diagnostics: '+str(log))
        return result.stdout
    job('certify_values.py')
    after=collect(policy,before['upper_lsn']);save(directory/'input/source-after.json',after)
    if fingerprint(before)!=fingerprint(after): raise RuntimeError('Source changed within the observed boundary; no proof imported')
    prepared=json.loads((directory/'output/marker-proofs.json').read_text())
    observed_hash=hashlib.sha256((directory/'input/source.json').read_bytes()).hexdigest()
    if prepared['source_observation_sha256']!=observed_hash or prepared['problems']: raise RuntimeError('Proof observation differs')
    # Input is a JSON document produced by the verified container, escaped as one SQL literal.
    body=json.dumps(prepared['proofs']).replace("'","''")
    count=sql("BEGIN; SET ROLE warehouse_owner; SELECT pg_advisory_xact_lock(782641901);\n"+
        "WITH inserted AS (INSERT INTO business.marker_proof(event_id,image,column_name,semantic_sha256,proof_sha256,source_observation_sha256,evidence) "
        "SELECT event_id,image,column_name,semantic_sha256,proof_sha256,source_observation_sha256,evidence FROM jsonb_to_recordset('"+body+"'::jsonb) "
        "AS p(event_id text,image text,column_name text,semantic_sha256 text,proof_sha256 text,source_observation_sha256 text,evidence jsonb) "
        "ON CONFLICT(event_id,image,column_name) DO NOTHING RETURNING event_id) SELECT count(*) FROM inserted; COMMIT;")
    job('resolve.py');job('certify_values.py','--verify')
    final=collect(policy,before['upper_lsn']);save(directory/'input/source-final.json',final)
    if fingerprint(before)!=fingerprint(final): raise RuntimeError('Source changed during derived comparison')
    comparison=json.loads((directory/'output/derived-comparison.json').read_text())
    report={k:v for k,v in comparison.items() if k!='proofs'}
    report.update(run_id=run_id,contract='cdc-value-reconstruction/v1',proofs_prepared=len(prepared['proofs']),
        new_proofs_imported=int(count.splitlines()[-1]),source_stable=True,source_fingerprint=fingerprint(before),
        completed_at=datetime.now(timezone.utc).isoformat(),protected_run_directory=str(directory))
    save(directory/'report.json',report)
    save(ROOT/'validation/cdc-value-reconstruction-v1-latest.json',report)
    print(json.dumps(report),flush=True)


if __name__=='__main__': main()
