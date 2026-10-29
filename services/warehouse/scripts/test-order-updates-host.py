"""Test the additive read API in a disposable database with isolated CDC fixtures."""
from pathlib import Path
import subprocess

service=Path(__file__).resolve().parents[1]
base=['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-v','ON_ERROR_STOP=1','-U','warehouse_admin']
subprocess.run(base+['-d','postgres'],input='CREATE DATABASE warehouse_sp_test;\n',text=True,check=True)
try:
    sql='GRANT CREATE ON DATABASE warehouse_sp_test TO warehouse_owner; GRANT CONNECT,TEMPORARY ON DATABASE warehouse_sp_test TO warehouse_loader; SET ROLE warehouse_owner;\n'
    sql+='\n'.join(p.read_text() for p in sorted((service/'sql').glob('[0-9][0-9][0-9]-*.sql')))
    subprocess.run(base+['-d','warehouse_sp_test'],input=sql,text=True,check=True)
    subprocess.run(['docker','compose','run','--rm','--no-deps','--user','0:0','-e','PGDATABASE=warehouse_sp_test',
        '-v','/srv/decom-sqlserver/secrets/warehouse/admin_password:/run/secrets/admin:ro',
        '-v','/srv/decom-sqlserver/secrets/warehouse/reader_password:/run/secrets/reader:ro',
        '-v',str(service/'sp-tests')+':/tests:ro','backfill','/bin/sh','-c',
        'pip install --disable-pip-version-check --no-cache-dir --target /tmp/test-deps pytest==9.0.2 >/dev/null && PYTHONPATH=/tmp/test-deps python -m pytest -p no:cacheprovider /tests -q'],cwd=service,check=True)
finally:
    subprocess.run(base+['-d','postgres'],input='DROP DATABASE warehouse_sp_test WITH (FORCE);\n',text=True,check=True)
