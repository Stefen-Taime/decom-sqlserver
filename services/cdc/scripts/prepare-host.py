"""Host-only secret generation and least-privilege SQL login provisioning."""
import base64
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

root = Path('/srv/decom-sqlserver')
secret_dir = root / 'secrets/cdc'
data_dir = root / 'cdc'
files = ['debezium_password', 'webhook', 'seaweed-s3.json', 's3-admin.json', 's3-archive.json']
if not secret_dir.exists():
    if data_dir.exists() and any(data_dir.rglob('*')):
        raise SystemExit('CDC data exists without its secrets. Restore the secrets; do not rotate silently.')
    secret_dir.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = Path(tempfile.mkdtemp(prefix='.cdc-', dir=secret_dir.parent))
    try:
        admin = {'access_key': 'admin-' + secrets.token_hex(12), 'secret_key': secrets.token_hex(32)}
        archive = {'access_key': 'archive-' + secrets.token_hex(12), 'secret_key': secrets.token_hex(32)}
        (temporary / 'debezium_password').write_text('Aa1!' + secrets.token_hex(24) + '\n')
        (temporary / 'webhook').write_text(base64.b64encode(secrets.token_bytes(32)).decode() + '\n')
        for name, values in [('s3-admin.json', admin), ('s3-archive.json', archive)]:
            (temporary / name).write_text(json.dumps(values) + '\n')
        identities = []
        for name, values, actions in [
            ('bootstrap', admin, ['Admin', 'Read', 'Write', 'List', 'Tagging']),
            ('cdc-archiver', archive, ['Read:cdc-raw', 'Write:cdc-raw', 'List:cdc-raw']),
        ]:
            identities.append({'name': name, 'credentials': [{
                'accessKey': values['access_key'], 'secretKey': values['secret_key']}], 'actions': actions})
        (temporary / 'seaweed-s3.json').write_text(json.dumps({'identities': identities}) + '\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
if not all((secret_dir / name).is_file() and (secret_dir / name).stat().st_size for name in files):
    raise SystemExit('Incomplete CDC secrets. Restore the missing files before continuing.')
secret_dir.chmod(0o700)
for name, uid, gid, mode in [
    ('debezium_password', 185, 0, 0o400), ('webhook', 0, 0, 0o444),
    ('seaweed-s3.json', 10002, 10002, 0o400),
    ('s3-admin.json', 0, 0, 0o400), ('s3-archive.json', 10003, 10003, 0o400),
]:
    path = secret_dir / name
    os.chown(path, uid, gid)
    path.chmod(mode)
for name, uid, gid in [('seaweedfs', 10002, 10002), ('debezium', 185, 0)]:
    path = data_dir / name
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, uid, gid)
    path.chmod(0o750)
password = (secret_dir / 'debezium_password').read_text().strip().replace("'", "''")
sql = Path('sql/connector-user.sql').read_text().replace('{{PASSWORD}}', password)
sql += '\n' + Path('../sqlserver/sql/06-lab-reader-roles.sql').read_text()
sql += """
DECLARE @expected bigint = (SELECT COUNT_BIG(*) FROM Sales.Customers), @visible bigint;
EXECUTE AS USER = N'debezium_cdc';
SELECT @visible = COUNT_BIG(*) FROM Sales.Customers;
REVERT;
IF @visible <> @expected THROW 51000, 'WWI customer visibility preflight failed', 1;
GO
"""
result = subprocess.run(['docker', 'exec', '-i', 'decom-sqlserver-sqlserver-1',
    '/bin/bash', '/opt/lab/bin/sqlcmd.sh'], input=sql, text=True, capture_output=True)
if result.returncode:
    # SQL errors can echo statements, so never print unsanitized output.
    print((result.stdout + result.stderr).replace(password, '[REDACTED]'))
    raise SystemExit('Connector login provisioning failed.')
print('CDC secrets, persistent directories and SQL login are ready.')
