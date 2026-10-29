"""Install the additive pilot after isolated tests, with a protected schema recovery point."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess

os.umask(0o077)
root=Path('/srv/decom-sqlserver')
service=Path(__file__).resolve().parents[1]
release=root/'releases/order-updates-v1-20261028'
release.mkdir(parents=True,exist_ok=True,mode=0o700)
backup=release/'pre-api-schema.dump'
if not backup.exists():
    with backup.open('xb') as output:
        subprocess.run(['docker','exec','decom-warehouse-postgres-1','pg_dump','-U','warehouse_admin','-d','warehouse','--schema-only','-Fc'],
            stdout=output,stdin=subprocess.DEVNULL,check=True)
    with backup.open('rb') as source:
        checksum=hashlib.file_digest(source,'sha256').hexdigest()
    (release/'recovery-point.json').write_text(json.dumps({'created_at':datetime.now(timezone.utc).isoformat(),
        'schema_backup':str(backup),'sha256':checksum,'scope':'Schema-only recovery artifact; this migration adds schemas, functions and a 14-row reference snapshot.',
        'disable_api_sql':'REVOKE EXECUTE ON FUNCTION app.get_order_updates(text,text,text) FROM warehouse_reader;',
        'full_database_restore_tested_in_this_step':False},indent=2)+'\n')
subprocess.run(['python3','scripts/migrate-host.py'],cwd=service,check=True)
subprocess.run(['python3','scripts/load-package-reference-host.py',str(root/'validation/sql-objects-20261028/package-types.json')],cwd=service,check=True)
print('Additive migration and versioned reference installed; no existing CDC checkpoint changed.',flush=True)
