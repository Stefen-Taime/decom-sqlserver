"""VM-only CDC recovery exercise, after the initial snapshot is complete."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

service = Path(__file__).resolve().parents[1]
offset = Path('/srv/decom-sqlserver/cdc/debezium/offsets.dat')


def compose(*args):
    return subprocess.run(['docker', 'compose', *args], cwd=service,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True).stdout


def probe():
    result = subprocess.check_output(['python3', str(service / 'scripts/probe-host.py')], text=True)
    return json.loads(result.strip().splitlines()[-1])['probe_id']


def wait_for_probe(probe_id):
    for _ in range(45):
        try:
            return json.loads(compose('exec', '-T', 'archiver', 'python', 'verify_archive.py', '--probe', probe_id))
        except subprocess.CalledProcessError:
            time.sleep(2)
    raise RuntimeError('Probe did not reach the verified archive: ' + probe_id)


baseline = probe()
wait_for_probe(baseline)
time.sleep(2)
print('Baseline insert/update/delete: PASS', flush=True)
compose('stop', 'seaweedfs')
try:
    before = hashlib.sha256(offset.read_bytes()).hexdigest()
    during_outage = probe()
    try:
        urllib.request.urlopen('http://127.0.0.1:8090/ready', timeout=30)
        raise AssertionError('Receiver was ready with storage offline')
    except urllib.error.HTTPError as error:
        assert error.code == 503, error.code
    time.sleep(15)
    assert hashlib.sha256(offset.read_bytes()).hexdigest() == before, 'Offset advanced while archive was offline'
    print('Storage outage: HTTP 503 and source offset unchanged', flush=True)
finally:
    compose('up', '-d', '--wait', '--wait-timeout', '180', 'seaweedfs')
outage_result = wait_for_probe(during_outage)
print('Storage recovery and backlog delivery: PASS', flush=True)
time.sleep(2)
compose('up', '-d', '--no-deps', '--force-recreate', '--wait', '--wait-timeout', '180', 'debezium')
after_recreate = probe()
restart_result = wait_for_probe(after_recreate)
logs = compose('logs', 'debezium')
assert 'Snapshot step 7 - Snapshotting data' not in logs, 'Unexpected fresh snapshot on recreation'
print('Connector recreation with persistent offsets: PASS', flush=True)
print(json.dumps({'baseline_probe': baseline, 'outage_probe': during_outage,
    'restart_probe': after_recreate, 'storage_outage_offset_unchanged': True,
    'purge_authorized': False, 'outage_verification': outage_result,
    'restart_verification': restart_result}, indent=2))
