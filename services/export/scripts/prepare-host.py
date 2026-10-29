"""Host-only credentials and S3 identities; never rotate on a redeploy."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

root = Path('/srv/decom-sqlserver')
secret_dir, data_dir = root/'secrets/export', root/'export'
config = root/'secrets/cdc/seaweed-s3.json'
stored = json.loads(config.read_text())
if not secret_dir.exists():
    if data_dir.exists() and any(data_dir.rglob('*.json')) or any(i['name'].startswith('export-') for i in stored['identities']):
        raise SystemExit('Export state/identity already exists; restore its secrets before proceeding.')
    temporary = Path(tempfile.mkdtemp(prefix='.export-', dir=secret_dir.parent))
    try:
        (temporary/'pg_password').write_text(secrets.token_hex(32)+'\n')
        for name in ['writer','consumer']:
            (temporary/f's3-{name}.json').write_text(json.dumps({
                'access_key':'export-'+name+'-'+secrets.token_hex(12),'secret_key':secrets.token_hex(32)})+'\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for filename, uid in [('pg_password',10005),('s3-writer.json',10005),('s3-consumer.json',10006)]:
    path = secret_dir/filename
    if not path.is_file() or not path.stat().st_size:
        raise SystemExit('Incomplete secrets; restore before proceeding.')
    path.chmod(0o400)
    os.chown(path,uid,uid)
for name, uid in [('requests',10005),('consumer',10006)]:
    path = data_dir/name
    path.mkdir(parents=True,exist_ok=True)
    path.chmod(0o700)
    os.chown(path,uid,uid)

changed = False
for name, actions in [('writer',['Read','Write','List']),('consumer',['Read','List'])]:
    values = json.loads((secret_dir/f's3-{name}.json').read_text())
    identity = {'name':'export-'+name, 'credentials':[{'accessKey':values['access_key'],
        'secretKey':values['secret_key']}], 'actions':[action+':warehouse-exports' for action in actions]}
    previous = next((i for i in stored['identities'] if i['name'] == identity['name']), None)
    if previous and previous != identity:
        raise SystemExit('Export S3 identity differs; restore or review explicitly.')
    if not previous:
        stored['identities'].append(identity)
        changed = True
if changed:
    backup = config.with_name('seaweed-s3.pre-export.json')
    if not backup.exists():
        shutil.copy2(config,backup)
        backup.chmod(0o400)
    # Update the existing bind-mounted inode. Restart the service before using new credentials.
    with config.open('w') as stream:
        stream.write(json.dumps(stored)+'\n'); stream.flush(); os.fsync(stream.fileno())
    Path('.reload-seaweed').touch(mode=0o600)

password = (secret_dir/'pg_password').read_text().strip()
statement = """BEGIN;
DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='warehouse_exporter')
 THEN CREATE ROLE warehouse_exporter LOGIN; END IF; END $$;
ALTER ROLE warehouse_exporter NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
 PASSWORD '%s';
ALTER ROLE warehouse_exporter SET default_transaction_read_only=on;
ALTER ROLE warehouse_exporter SET statement_timeout='10min';
ALTER ROLE warehouse_exporter SET search_path=pg_catalog;
GRANT CONNECT ON DATABASE warehouse TO warehouse_exporter;
GRANT USAGE ON SCHEMA delivery TO warehouse_exporter;
GRANT SELECT ON delivery.invoice_details_v1,delivery.order_details_v1,delivery.ingestion_context TO warehouse_exporter;
COMMIT;
""" % password.replace("'","''")
result = subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-v',
    'ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],input=statement,text=True,capture_output=True)
if result.returncode:
    raise SystemExit((result.stdout+result.stderr).replace(password,'[REDACTED]'))
Path('.env').write_text(f'EXPORT_DATA_DIR={data_dir}\nEXPORT_SECRET_DIR={secret_dir}\n')
Path('.env').chmod(0o600)
print('Scoped export identities and persistent state prepared; no credentials printed.')
