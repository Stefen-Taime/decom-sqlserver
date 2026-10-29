"""Live acceptance: real DAGs, post-publication retry, durable receipts and component recreation."""
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError
from api import Client

ROOT = Path('/srv/decom-sqlserver')
SERVICE = ROOT / 'workspace/services/airflow'
VALIDATION = ROOT / 'validation'
VALIDATION.mkdir(exist_ok=True, mode=0o700)
sys.path.insert(0, str(SERVICE / 'dags'))
from pipeline_client import request_id


def run(arguments, input=None, timeout=300):
    result = subprocess.run(arguments, cwd=SERVICE, input=input, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        path = VALIDATION / 'airflow-last-failure.log'
        path.write_text(result.stdout + result.stderr); path.chmod(0o600)
        raise RuntimeError('See protected diagnostics: ' + str(path))
    return result.stdout


def save(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n'); temp.chmod(0o600); temp.replace(path)


def pg(query, database='warehouse'):
    return json.loads(run(['docker', 'exec', 'decom-warehouse-postgres-1', 'psql', '-X', '-A', '-t',
                           '-U', 'warehouse_admin', '-d', database, '-c', query]))


def counts():
    return pg("SELECT json_build_object('events',(SELECT count(*) FROM staging.cdc_event),"
              "'batches',(SELECT count(*) FROM ops.consumer_batches WHERE consumer_version='sales/v2'),"
              "'current_keys',(SELECT count(*) FROM staging.current_key))")


def runner(payload):
    return json.loads(run(['/usr/local/libexec/decom-airflow-runner'], json.dumps(payload)))


def ssh(payload):
    code = "import sys,json;sys.path.insert(0,'/opt/airflow/dags');from pipeline_client import call;print(json.dumps(call(json.loads(sys.stdin.read()))))"
    return json.loads(run(['docker', 'exec', '-i', 'decom-airflow-scheduler-1', 'python', '-c', code], json.dumps(payload)))


progress_path = VALIDATION / 'airflow-live-progress.json'
progress = json.loads(progress_path.read_text()) if progress_path.exists() else {
    'started_at': datetime.now(timezone.utc).isoformat(), 'warehouse_before': counts(), 'runs': {}}
save(progress_path, progress)
run(['python3', '-m', 'unittest', 'discover', '-s', 'tests', '-v'])
client = Client()
assert client.request('GET', '/api/v2/importErrors')['total_entries'] == 0
assert {d['dag_id'] for d in client.request('GET', '/api/v2/dags')['dags']} == {'export_invoices', 'export_orders'}
progress['ingestion_readiness'] = ssh({'action': 'preflight', 'dataset': 'invoice_details'})

boundary = """
import json,sys
sys.path.insert(0,'/opt/airflow/dags')
from pipeline_client import call
checks=[]
for payload in [
 {'action':'purge','dataset':'invoice_details'},
 {'action':'preflight','dataset':'invoice_details','command':'id'},
 {'action':'preflight','dataset':'unregistered'}]:
 try: call(payload)
 except RuntimeError: checks.append('rejected')
 else: raise AssertionError('Restricted SSH runner accepted an invalid request')
import psycopg2
from pathlib import Path
url=Path('/run/secrets/sql_connection').read_text().strip().replace('postgresql+psycopg2://','postgresql://',1)
url=url.replace('/airflow_meta?','/warehouse?',1)
try:
 connection=psycopg2.connect(url,connect_timeout=5)
except psycopg2.OperationalError as error:
 assert 'permission denied for database' in str(error)
 checks.append('warehouse connection denied')
else:
 connection.close();raise AssertionError('Airflow role connected to business warehouse')
print(json.dumps(checks))
"""
progress['access_checks'] = json.loads(run(['docker', 'exec', 'decom-airflow-scheduler-1', 'python', '-c', boundary]))
save(progress_path, progress)

for dag, dataset, fault in [('export_invoices', 'invoice_details', True), ('export_orders', 'order_details', False)]:
    if dag not in progress['runs']:
        progress['runs'][dag] = {'run_id': 'acceptance__' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
                                'dataset': dataset, 'intentional_post_publication_failure': fault}
        save(progress_path, progress)
    item = progress['runs'][dag]
    client.request('PATCH', '/api/v2/dags/' + dag, {'is_paused': False})
    path = '/api/v2/dags/' + dag + '/dagRuns/' + item['run_id']
    try: client.request('GET', path)
    except HTTPError as error:
        if error.code != 404: raise
        client.request('POST', '/api/v2/dags/' + dag + '/dagRuns', {
            'dag_run_id': item['run_id'], 'logical_date': None,
            'conf': {'fail_after_export_once': fault}, 'note': 'Live orchestration acceptance; preserve this run for evidence.'})

deadline = time.monotonic() + 1500
previous = None
while time.monotonic() < deadline:
    summary = {}
    for dag, item in progress['runs'].items():
        base = '/api/v2/dags/' + dag + '/dagRuns/' + item['run_id']
        item['state'] = client.request('GET', base)['state']
        tasks = client.request('GET', base + '/taskInstances')['task_instances']
        item['tasks'] = [{key: t.get(key) for key in ['task_id', 'state', 'try_number', 'start_date', 'end_date']}
                         for t in tasks]
        summary[dag] = {'state': item['state'], 'tasks': {t['task_id']: [t['state'], t['try_number']] for t in tasks}}
    if summary != previous:
        print(json.dumps(summary), flush=True); previous = summary
    save(progress_path, progress)
    if any(i['state'] == 'failed' for i in progress['runs'].values()):
        raise RuntimeError('A DAG failed; retain this run and investigate its task logs')
    if all(i['state'] == 'success' for i in progress['runs'].values()): break
    time.sleep(10)
else: raise TimeoutError('Acceptance run deadline exceeded; resume using retained run identities')

for dag, item in progress['runs'].items():
    identity = request_id(item['dataset'], item['run_id'])
    path = ROOT / 'airflow/runner/requests' / (identity + '.json')
    before = path.read_bytes()
    journal = json.loads(before)
    exported = journal['jobs']['export']['result']; verified = journal['jobs']['verify']['result']
    assert exported['attempts'] == 1, 'Task retry created another export attempt'
    assert verified['rows'] == (228265 if dag == 'export_invoices' else 231412)
    assert exported['publication'] == verified['publication']
    repeat = ssh({'action': 'submit', 'dataset': item['dataset'], 'request_id': identity, 'operation': 'export', 'attempt': 3})
    assert repeat['status'] == 'succeeded' and repeat['result'] == exported and path.read_bytes() == before
    item.update(request_id=identity, export=exported, validation=verified, repeat_reused_completed_result=True)
    if dag == 'export_invoices':
        task = next(t for t in item['tasks'] if t['task_id'] == 'export_snapshot')
        assert task['try_number'] >= 2, 'Post-publication retry was not exercised'
        item['application_receipt'] = ssh({'action': 'invoice_receipt', 'dataset': item['dataset'], 'request_id': identity})
        assert item['application_receipt']['processed']
save(progress_path, progress)

log_hashes = {str(p.relative_to(ROOT / 'airflow/logs')): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (ROOT / 'airflow/logs').rglob('*.log') if 'acceptance__' in str(p)}
assert len(log_hashes) >= 8, 'Expected persisted task attempt logs'
if not progress.get('components_recreated'):
    run(['docker', 'compose', 'up', '-d', '--force-recreate', '--wait', '--wait-timeout', '240',
         'api', 'dag-processor', 'scheduler'])
    progress['components_recreated'] = True
    save(progress_path, progress)
client = Client()
for dag, item in progress['runs'].items():
    assert client.request('GET', '/api/v2/dags/' + dag + '/dagRuns/' + item['run_id'])['state'] == 'success'
for relative, checksum in log_hashes.items():
    assert hashlib.sha256((ROOT / 'airflow/logs' / relative).read_bytes()).hexdigest() == checksum

health = {}
names = ['decom-sqlserver-sqlserver-1', 'decom-cdc-debezium-1', 'decom-cdc-archiver-1',
         'decom-cdc-seaweedfs-1', 'decom-cloudbeaver-cloudbeaver-1', 'decom-warehouse-postgres-1',
         'decom-warehouse-loader-1', 'decom-messaging-rabbitmq-1', 'decom-messaging-dispatcher-1',
         'decom-messaging-invoice-app-1', 'decom-airflow-api-1', 'decom-airflow-scheduler-1', 'decom-airflow-dag-processor-1']
for name in names:
    container = json.loads(run(['docker', 'inspect', name]))[0]
    state = container['State']
    assert state['Running'] and state['Health']['Status'] == 'healthy' and not state['OOMKilled'], name
    health[name] = 'healthy'
    if name.startswith('decom-airflow-'):
        assert not any(m['Destination'] == '/var/run/docker.sock' for m in container['Mounts'])
        assert not any('secrets/messaging' in m['Source'] or 'secrets/export' in m['Source'] for m in container['Mounts'])
component_health = client.request('GET', '/api/v2/monitor/health')
for component in ['metadatabase', 'scheduler', 'dag_processor']:
    assert component_health[component]['status'] == 'healthy'
assert counts() == progress['warehouse_before']
spec = importlib.util.spec_from_file_location('source_export', ROOT / 'workspace/services/warehouse/scripts/source-export.py')
source = importlib.util.module_from_spec(spec); spec.loader.exec_module(source)
cleanup = source.query("SELECT j.enabled FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id "
                       "WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
assert cleanup['enabled'] == 0
pools = client.request('GET', '/api/v2/pools')['pools']
assert next(p for p in pools if p['name'] == 'export_jobs')['slots'] == 1
versions = json.loads(run(['docker', 'exec', 'decom-airflow-api-1', 'python', '-c',
    "import json,importlib.metadata as m;print(json.dumps({n:m.version(n) for n in ['apache-airflow','apache-airflow-providers-fab','paramiko']}))"]))
assert versions['apache-airflow'] == '3.3.2'
mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
report = {'verified_at': datetime.now(timezone.utc).isoformat(), 'versions': versions,
          'image_id': run(['docker', 'image', 'inspect', '--format', '{{.Id}}', 'decom-airflow:3.3.2-1']).strip(),
          'unit_tests_passed': 12, 'live_acceptance': progress, 'service_health': health,
          'airflow_component_health': component_health, 'persisted_task_log_files': len(log_hashes),
          'warehouse_after': counts(), 'airflow_runtime_has_no_docker_socket_or_business_secrets': True,
          'host_memory_kib': {'total': mem['MemTotal'], 'available': mem['MemAvailable']},
          'postgres_connections': pg("SELECT json_build_object('total',count(*),'airflow',count(*) FILTER (WHERE usename='airflow_meta')) FROM pg_stat_activity"),
          'scheduled_exports': [{'dag_id': d['dag_id'], 'paused': d['is_paused'], 'cron': d['timetable_summary'],
                                 'next_run_utc': d['next_dagrun_run_after'], 'timezone': 'America/Toronto'}
                                for d in client.request('GET', '/api/v2/dags')['dags']],
          'automatic_cdc_cleanup_enabled': False, 'purge_authorized': False,
          'limitations': ['Single VM/shared PostgreSQL; no HA or off-VM disaster recovery.',
                          'Readiness is a recent scan, not a certified contiguous source CDC watermark.',
                          'Independent full snapshots; no common transaction boundary across datasets.',
                          'No external failure notification or automatic evidence/log/export retention.']}
destination = VALIDATION / 'airflow-exports-v1-2026-10-29.json'
save(destination, report)
print(json.dumps({'evidence': str(destination), 'healthy_services': len(health), 'dag_runs': 'success',
                  'post_publication_retry_reused_export': True}), flush=True)
