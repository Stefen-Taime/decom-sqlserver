"""VM-only graceful Debezium recreation with isolated lab probes and retained evidence.

This intentionally pauses capture briefly. It never resets offsets, stops storage,
changes a business row, or enables cleanup. Crash and off-VM recovery are not tested.
"""
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

ROOT=Path('/srv/decom-sqlserver');SERVICE=ROOT/'workspace/services/cdc'
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
spec=importlib.util.spec_from_file_location('checkpoint_input',HERE/'checkpoint-input.py')
checkpoint=importlib.util.module_from_spec(spec);spec.loader.exec_module(checkpoint)
from offset_checkpoint import position
from resume_audit import SourcePositions
from source import collect


def save(path,value):
    temporary=path.with_name(path.name+'.tmp')
    with temporary.open('wb') as stream:
        stream.write(value if isinstance(value,bytes) else (json.dumps(value,indent=2)+'\n').encode())
        stream.flush();os.fsync(stream.fileno())
    temporary.chmod(0o400);os.replace(temporary,path)
    fd=os.open(path.parent,os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def main():
    if os.geteuid()!=0: raise SystemExit('Run with sudo on the VM')
    os.umask(0o077)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    directory=ROOT/'validation'/('cdc-resume-'+run_id);directory.mkdir(mode=0o700)
    def run(command,timeout=600):
        result=subprocess.run(command,cwd=SERVICE,text=True,capture_output=True,timeout=timeout)
        log=directory/('command-'+uuid.uuid4().hex[:8]+'.log')
        save(log,(result.stdout+result.stderr).encode())
        if result.returncode: raise RuntimeError('Resume exercise command failed; protected log: '+str(log))
        return result.stdout
    def compose(*args): return run(['docker','compose',*args])
    def archive(*args):
        return json.loads(compose('exec','-T','archiver','python','verify_archive.py',*args))
    def probe():
        return json.loads(run(['python3',str(SERVICE/'scripts/probe-host.py')]).strip().splitlines()[-1])['probe_id']
    def wait_probe(identity):
        for _ in range(45):
            try: return archive('--probe',identity)
            except RuntimeError: time.sleep(2)
        raise RuntimeError('Probe did not reach its verified archive')
    def saved_state(label,flush=False):
        observation,data=checkpoint.state(flush=True) if flush else checkpoint.stable_state()
        for name,value in data.items(): save(directory/(label+'-'+name),value)
        save(directory/(label+'-state.json'),observation);return observation
    try:
        # Fail before touching the source or connector if its format/mount is unsupported.
        initial,_=checkpoint.stable_state();position(initial['checkpoint'])
        if checkpoint.inspect('decom-cdc-debezium-1')['state'].get('Health',{}).get('Status')!='healthy':
            raise ValueError('Connector must be healthy before the exercise')
        snapshot_before=archive('--snapshot')
        ids={'before':probe()};wait_probe(ids['before'])
        try:
            compose('stop','debezium')
            before=saved_state('before',flush=True)
            ids['while_stopped']=probe()
        finally:
            # Recreate even when evidence capture failed, retaining the existing bind mount.
            compose('up','-d','--no-deps','--force-recreate','--wait','--wait-timeout','180','debezium')
        wait_probe(ids['while_stopped'])
        ids['after']=probe();verification=wait_probe(ids['after'])
        policy=json.loads((HERE.parent/'policy.json').read_text())
        binding=SourcePositions(collect(policy),policy,before['checkpoint']['command_id'] is not None)
        positions=[]
        for event in verification['probes'][ids['after']]:
            row={'ProbeID':ids['after'],'Payload':event['value']}
            envelope={'op':event['op'],'before':row,'after':row,
                      'source':{**event,'schema':'lab','table':'CdcProbe'}}
            positions.append(binding.for_event(envelope))
        last=max(positions)
        for _ in range(30):
            after,_=checkpoint.stable_state()
            if position(after['checkpoint'])>=last: break
            time.sleep(1)
        else: raise ValueError('Durable file did not catch up with the probe')
        after=saved_state('after');snapshot_after=archive('--snapshot')
        if (before['checkpoint']['command_id'] is None)!=(after['checkpoint']['command_id'] is None):
            raise ValueError('Source query mode changed')
        if before['container_id']==after['container_id'] or before['image_id']!=after['image_id']:
            raise ValueError('Container was not recreated with the same image')
        for name in ['config_sha256','state_directory','source_container_id']:
            if before[name]!=after[name]: raise ValueError('Exercise environment changed')
        if before['files']['schema-history.dat']!=after['files']['schema-history.dat']:
            raise ValueError('Schema history changed during exercise')
        if not position(before['checkpoint'])<position(after['checkpoint']): raise ValueError('Checkpoint did not advance')
        if not snapshot_before['operations'].get('r',0)==snapshot_after['operations'].get('r',0)>0:
            raise ValueError('Snapshot record count changed')
        report={'contract':'cdc-local-resume-exercise/v1','run_id':run_id,'passed':True,
            'completed_at':datetime.now(timezone.utc).isoformat(),'before':before,'after':after,
            'probe_ids':ids,'state_fsynced_while_stopped':True,'mounts_preserved':True,
            'snapshot_records_before':snapshot_before['operations']['r'],
            'snapshot_records_after':snapshot_after['operations']['r'],
            'snapshot_unique_keys':snapshot_after['snapshot_unique_keys'],
            'purge_authorized':False,'crash_durability_certified':False,'transaction_fence_certified':False,
            'provenance_sha256':{str(p.relative_to(HERE.parent)):hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in sorted(HERE.parent.rglob('*.py'))}}
        save(directory/'report.json',report)
        save(ROOT/'validation/cdc-resume-v1-latest.json',{'report':str(directory/'report.json'),
            'sha256':hashlib.sha256((directory/'report.json').read_bytes()).hexdigest()})
        print(json.dumps({'run_id':run_id,'report':str(directory/'report.json'),'passed':True,'purge_authorized':False}))
    except Exception as error:
        save(directory/'failure.json',{'run_id':run_id,'passed':False,'error_type':type(error).__name__,
                                      'purge_authorized':False})
        raise


if __name__=='__main__':
    with (ROOT/'purge/audit.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main()
