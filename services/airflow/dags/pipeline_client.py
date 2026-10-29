"""Restricted SSH transport and stable delivery identities; no direct Docker or data credentials."""
import hashlib
import json
import time


def request_id(dataset,dag_run_id):
    return 'af1-'+hashlib.sha256(('export-orchestration/v1\0'+dataset+'\0'+dag_run_id).encode()).hexdigest()[:40]


def call(payload):
    import paramiko
    client=paramiko.SSHClient()
    client.load_host_keys('/run/secrets/runner_known_hosts')
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect('lab-host',username='airflow-runner',key_filename='/run/secrets/runner_key',
                       look_for_keys=False,allow_agent=False,timeout=10,auth_timeout=10,banner_timeout=10)
        stdin,stdout,stderr=client.exec_command('pipeline-request/v1',timeout=90)
        stdin.write(json.dumps(payload));stdin.flush();stdin.channel.shutdown_write()
        raw=stdout.read(32769)
        if len(raw)>32768: raise RuntimeError('Oversized runner response')
        code=stdout.channel.recv_exit_status()
        value=json.loads(raw)
        if code or value.get('status')=='error':
            raise RuntimeError('Restricted runner rejected operation: '+value.get('error_type','unknown'))
        return value
    finally: client.close()


def job(dataset,identity,operation,attempt,timeout=900):
    deadline=time.monotonic()+timeout
    base={'dataset':dataset,'request_id':identity,'operation':operation}
    while time.monotonic()<deadline:
        value=call({**base,'action':'submit','attempt':attempt})
        if value['status']=='succeeded': return value['result']
        if value['status']=='failed': raise RuntimeError('Export job failed; retry retains the same request identity')
        if value['status']=='busy':
            time.sleep(5);continue
        while time.monotonic()<deadline:
            time.sleep(5)
            value=call({**base,'action':'status'})
            if value['status']=='succeeded': return value['result']
            if value['status']=='failed': raise RuntimeError('Export job failed; inspect protected runner log')
    raise TimeoutError('Task deadline exceeded; submitted job remains recoverable by the same request identity')
