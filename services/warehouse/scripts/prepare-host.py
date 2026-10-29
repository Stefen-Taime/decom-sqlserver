"""Prepare persistent PostgreSQL state, scoped identities and immutable generated secrets."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

root = Path('/srv/decom-sqlserver')
secret_dir, data_dir = root/'secrets/warehouse', root/'warehouse'
if not secret_dir.exists():
    if data_dir.exists() and any(data_dir.rglob('PG_VERSION')):
        raise SystemExit('Existing warehouse without its secrets; restore secrets before redeploying.')
    temporary = Path(tempfile.mkdtemp(prefix='.warehouse-', dir=secret_dir.parent))
    try:
        for filename in ['admin_password','loader_password','reader_password']:
            (temporary/filename).write_text(secrets.token_hex(32)+'\n')
        (temporary/'s3-reader.json').write_text(json.dumps({'access_key':'warehouse-'+secrets.token_hex(12), 'secret_key':secrets.token_hex(32)})+'\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for filename,uid in [('admin_password',999),('loader_password',10004),('reader_password',0),('s3-reader.json',10004)]:
    path = secret_dir/filename
    if not path.is_file() or not path.stat().st_size:
        raise SystemExit('Incomplete warehouse secrets; restore before proceeding.')
    os.chown(path,uid,uid)
    path.chmod(0o400)
pgdata = data_dir/'postgres'
pgdata.mkdir(parents=True, exist_ok=True)
os.chown(pgdata,999,999)
pgdata.chmod(0o700)
network = subprocess.run(['docker','network','inspect','decom-warehouse-backend'], capture_output=True, text=True)
if network.returncode:
    subprocess.run(['docker','network','create','--internal','decom-warehouse-backend'], check=True, stdout=subprocess.DEVNULL)
elif not json.loads(network.stdout)[0]['Internal']:
    raise SystemExit('Warehouse network must be internal.')
s3_config = root/'secrets/cdc/seaweed-s3.json'
stored = json.loads(s3_config.read_text())
credentials = json.loads((secret_dir/'s3-reader.json').read_text())
identity = {'name':'warehouse-reader','credentials':[{'accessKey':credentials['access_key'],'secretKey':credentials['secret_key']}], 'actions':['Read:cdc-raw','List:cdc-raw']}
existing = next((i for i in stored['identities'] if i['name']=='warehouse-reader'), None)
if existing and existing != identity:
    raise SystemExit('Warehouse S3 identity differs; restore or review credentials explicitly.')
if not existing:
    backup = s3_config.with_name('seaweed-s3.pre-warehouse.json')
    if not backup.exists():
        shutil.copy2(s3_config, backup)
        backup.chmod(0o400)
    stored['identities'].append(identity)
    s3_config.write_text(json.dumps(stored)+'\n')
    Path('.reload-seaweed').touch(mode=0o600)
Path('.env').write_text(f'WAREHOUSE_DATA_DIR={data_dir}\nWAREHOUSE_SECRET_DIR={secret_dir}\n')
Path('.env').chmod(0o600)
print('Warehouse persistent storage, network and secrets prepared; no credentials printed.')
