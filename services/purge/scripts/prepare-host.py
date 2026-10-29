"""Provision audit-only identities. No cleanup permission or destructive SQL is installed."""
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

ROOT = Path('/srv/decom-sqlserver')
secret_dir = ROOT / 'secrets/purge'
data_dir = ROOT / 'purge'
if not secret_dir.exists():
    if data_dir.exists() and any(data_dir.rglob('*')):
        raise SystemExit('Existing audit state without secrets; restore them first.')
    temporary = Path(tempfile.mkdtemp(prefix='.purge-', dir=secret_dir.parent))
    try:
        (temporary/'sql_password').write_text('Aud1!'+secrets.token_hex(32)+'\n')
        (temporary/'pg_password').write_text(secrets.token_hex(32)+'\n')
        (temporary/'s3-reader.json').write_text(json.dumps({'access_key':'cdc-audit-'+secrets.token_hex(12), 'secret_key':secrets.token_hex(32)})+'\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists(): shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for name in ['sql_password','pg_password','s3-reader.json']:
    path=secret_dir/name
    if not path.is_file() or not path.stat().st_size: raise SystemExit('Incomplete audit secrets; restore them.')
    os.chown(path,0 if name=='sql_password' else 10010,0 if name=='sql_password' else 10010);path.chmod(0o400)
data_dir.mkdir(exist_ok=True,mode=0o700)
(data_dir/'runs').mkdir(exist_ok=True,mode=0o700)
spec=importlib.util.spec_from_file_location('source_admin',ROOT/'workspace/services/warehouse/scripts/source-export.py')
admin=importlib.util.module_from_spec(spec);spec.loader.exec_module(admin)
password=(secret_dir/'sql_password').read_text().strip()
if not password.startswith('Aud1!') or not all(c in '0123456789abcdef' for c in password[5:]):
    raise SystemExit('Unexpected SQL audit password encoding')
admin.query(f"""
USE master;
IF SUSER_ID(N'cdc_auditor') IS NULL CREATE LOGIN cdc_auditor WITH PASSWORD=N'{password}',CHECK_POLICY=ON,CHECK_EXPIRATION=OFF;
IF USER_ID(N'cdc_auditor') IS NULL CREATE USER cdc_auditor FOR LOGIN cdc_auditor;
DENY EXECUTE ON OBJECT::sys.sp_cdc_cleanup_change_table TO cdc_auditor;
USE WideWorldImporters;
IF USER_ID(N'cdc_auditor') IS NULL CREATE USER cdc_auditor FOR LOGIN cdc_auditor;
GRANT CONNECT TO cdc_auditor;
GRANT VIEW DEFINITION TO cdc_auditor;
GRANT SELECT ON SCHEMA::cdc TO cdc_auditor;
DENY INSERT,UPDATE,DELETE ON SCHEMA::cdc TO cdc_auditor;
DENY INSERT,UPDATE,DELETE ON SCHEMA::Sales TO cdc_auditor;
DENY INSERT,UPDATE,DELETE ON SCHEMA::lab TO cdc_auditor;
SELECT 1 AS audit_identity_prepared FOR JSON PATH,WITHOUT_ARRAY_WRAPPER;
""")
pg_password=(secret_dir/'pg_password').read_text().strip()
if not all(c in '0123456789abcdef' for c in pg_password): raise SystemExit('Unexpected PostgreSQL password encoding')
sql=f"""
DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='warehouse_cdc_auditor') THEN CREATE ROLE warehouse_cdc_auditor LOGIN; END IF; END $$;
ALTER ROLE warehouse_cdc_auditor NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 2 PASSWORD '{pg_password}';
ALTER ROLE warehouse_cdc_auditor IN DATABASE warehouse SET default_transaction_read_only=on;
GRANT CONNECT ON DATABASE warehouse TO warehouse_cdc_auditor;
GRANT USAGE ON SCHEMA ops,staging TO warehouse_cdc_auditor;
GRANT SELECT ON ops.archive_batches,ops.consumer_batches,ops.consumer_status TO warehouse_cdc_auditor;
GRANT SELECT(event_id,semantic_sha256,stream_phase,commit_lsn) ON staging.cdc_event TO warehouse_cdc_auditor;
"""
result=subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],
                      input=sql,text=True,capture_output=True)
if result.returncode: raise RuntimeError((result.stdout+result.stderr).replace(pg_password,'[REDACTED]'))
config=ROOT/'secrets/cdc/seaweed-s3.json'
stored=json.loads(config.read_text());credentials=json.loads((secret_dir/'s3-reader.json').read_text())
identity={'name':'cdc-purge-auditor','credentials':[{'accessKey':credentials['access_key'],'secretKey':credentials['secret_key']}],
          'actions':['List:cdc-raw','Read:cdc-raw/raw/wwi/bootstrap-v2/*']}
previous=next((i for i in stored['identities'] if i['name']==identity['name']),None)
if previous and previous!=identity: raise SystemExit('Existing S3 audit identity differs; review explicitly.')
if not previous:
    backup=config.with_name('seaweed-s3.pre-purge-audit.json')
    if not backup.exists(): shutil.copy2(config,backup);backup.chmod(0o400)
    stored['identities'].append(identity)
    with config.open('w') as stream:
        stream.write(json.dumps(stored)+'\n');stream.flush();os.fsync(stream.fileno())
    Path('.reload-seaweed').touch(mode=0o600)
Path('.env').write_text(f'PURGE_SECRET_DIR={secret_dir}\n');Path('.env').chmod(0o600)
print('Dedicated SQL, warehouse and S3 read-only audit identities prepared.')
