import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

root = Path('/srv/decom-sqlserver')
secret_dir = root / 'secrets/cloudbeaver'
workspace = root / 'cloudbeaver'
if not secret_dir.exists():
    if workspace.exists() and any(workspace.iterdir()):
        raise SystemExit('Existing CloudBeaver workspace without secrets: restore secrets before continuing.')
    secret_dir.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = Path(tempfile.mkdtemp(prefix='.cloudbeaver-', dir=secret_dir.parent))
    try:
        for filename in ['admin_password', 'sql_reader_password']:
            (temporary / filename).write_text('Aa1!' + secrets.token_hex(20) + '\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for filename, uid in [('admin_password', 8978), ('sql_reader_password', 0)]:
    path = secret_dir / filename
    if not path.is_file() or not path.stat().st_size:
        raise SystemExit('Incomplete secrets. Restore them before continuing.')
    os.chown(path, uid, uid)
    path.chmod(0o400)
password = (secret_dir / 'sql_reader_password').read_text().strip()
sql = Path('sql/reader-user.sql').read_text().replace('{{PASSWORD}}', password.replace("'", "''"))
sql += '\n' + Path('../sqlserver/sql/06-lab-reader-roles.sql').read_text()
result = subprocess.run(['docker', 'exec', '-i', 'decom-sqlserver-sqlserver-1',
    '/bin/bash', '/opt/lab/bin/sqlcmd.sh'], input=sql, text=True, capture_output=True)
if result.returncode:
    print((result.stdout + result.stderr).replace(password, '[REDACTED]'))
    raise SystemExit('SQL reader provisioning failed.')
connection_dir = workspace / 'GlobalConfiguration/.dbeaver'
connection_dir.mkdir(parents=True, exist_ok=True)
connections_file = connection_dir / 'data-sources.json'
if not connections_file.exists():
    connections_file.write_text(json.dumps({'folders': {}, 'connections': {
        'wwi-sqlserver': {
            'provider': 'sqlserver', 'driver': 'microsoft',
            'name': 'WideWorldImporters — SQL Server (read only)',
            'save-password': True, 'read-only': True,
            'configuration': {
                'host': 'sqlserver', 'port': '1433', 'database': 'WideWorldImporters',
                'url': 'jdbc:sqlserver://sqlserver:1433;databaseName=WideWorldImporters;encrypt=true;trustServerCertificate=true',
                'configurationType': 'URL', 'type': 'dev', 'auth-model': 'native',
                'auth-properties': {'userName': 'lab_reader', 'userPassword': password},
            },
            'navigator-settings': {'showOnlyEntities': False, 'hideFolders': False},
        }
    }}, indent=2) + '\n')
    connections_file.chmod(0o600)
# Repair the first bootstrap's obsolete driver identifier; preserve all other connections.
# Run with CloudBeaver stopped if this legacy entry exists.
stored = json.loads(connections_file.read_text())
legacy = stored.get('connections', {}).get('wwi-sqlserver', {})
if legacy.get('provider') == 'sqlserver' and legacy.get('driver') == 'mssql':
    legacy['driver'] = 'microsoft'
    legacy['configuration']['auth-properties'] = {'userName': 'lab_reader', 'userPassword': password}
    connections_file.write_text(json.dumps(stored, indent=2) + '\n')
warehouse_password = root / 'secrets/warehouse/reader_password'
if warehouse_password.exists() and 'warehouse-postgres' not in stored.get('connections', {}):
    # Add only this managed connection; preserve user connections and existing credentials.
    stored.setdefault('connections', {})['warehouse-postgres'] = {
        'provider': 'postgresql', 'driver': 'postgres-jdbc',
        'name': 'Warehouse — PostgreSQL (read only)', 'save-password': True, 'read-only': True,
        'configuration': {'host': 'postgres', 'port': '5432', 'database': 'warehouse',
            'url': 'jdbc:postgresql://postgres:5432/warehouse',
            'configurationType': 'URL', 'type': 'dev', 'auth-model': 'native',
            'auth-properties': {'userName': 'warehouse_reader', 'userPassword': warehouse_password.read_text().strip()}},
        'navigator-settings': {'showOnlyEntities': False, 'hideFolders': False},
    }
    connections_file.write_text(json.dumps(stored, indent=2) + '\n')
    connections_file.chmod(0o600)
# User-created connections and encrypted credentials survive subsequent deployments.
for directory in [workspace, workspace / 'GlobalConfiguration', connection_dir]:
    os.chown(directory, 8978, 8978)
    directory.chmod(0o700)
os.chown(connections_file, 8978, 8978)
access = secret_dir / 'access.txt'
access.write_text('CloudBeaver: http://localhost:8978/\nUser: stefen\nPassword: ' +
    (secret_dir / 'admin_password').read_text().strip() +
    '\n\nThe prepared SQL Server connection uses lab_reader (read only).\n')
access.chmod(0o600)
print('CloudBeaver secrets and SQL connection are prepared. Access details: ' + str(access))
