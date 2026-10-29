"""Verify a same-VM logical backup in an isolated database; this is not off-VM disaster recovery."""
from datetime import datetime,timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

os.umask(0o077)
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--sales-source',help='Five-table streaming source baseline directory')
args=parser.parse_args()
service=Path(__file__).resolve().parents[1]
root=Path('/srv/decom-sqlserver')
directory=root/'warehouse/backups'
directory.mkdir(parents=True,exist_ok=True,mode=0o700)
backup=directory/('warehouse-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'.dump')
base=['docker','exec','-i','decom-warehouse-postgres-1']
with backup.open('xb') as output:
    subprocess.run(base+['pg_dump','-U','warehouse_admin','-d','warehouse','-Fc'],stdout=output,check=True)
    output.flush()
    os.fsync(output.fileno())
psql=base+['psql','-X','-q','-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','postgres']
subprocess.run(psql,input='CREATE DATABASE warehouse_restore;\n',text=True,check=True)
try:
    with backup.open('rb') as source:
        subprocess.run(base+['pg_restore','--exit-on-error','-U','warehouse_admin','-d','warehouse_restore'],stdin=source,check=True)
    subprocess.run(base+['vacuumdb','--analyze-only','--quiet','-U','warehouse_admin','-d','warehouse_restore'],check=True)
    report=root/('validation/warehouse-sales-v2-restored.json' if args.sales_source else 'validation/warehouse-restored-reconciliation.json')
    command=['python3',str(service/'scripts/reconcile-host.py'),args.sales_source or str(root/'validation/source-pilot-after.json'),
        '--database','warehouse_restore','--output',str(report)]
    if args.sales_source:
        command.append('--sales')
    subprocess.run(command,check=True)
    with backup.open('rb') as source:
        checksum=hashlib.file_digest(source,'sha256').hexdigest()
    result={'backup':str(backup),'sha256':checksum,
        'bytes':backup.stat().st_size,'isolated_restore_and_business_reconciliation':'passed',
        'existing_cluster_roles_reused':True,'off_vm_recovery_tested':False,'purge_authorized':False}
    (root/('validation/warehouse-sales-v2-backup.json' if args.sales_source else 'validation/warehouse-backup-restore.json')).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
finally:
    subprocess.run(psql,input='DROP DATABASE warehouse_restore WITH (FORCE);\n',text=True,check=True)
