"""Run integration tests in a disposable database; the production pilot is untouched."""
from pathlib import Path
import subprocess

service = Path(__file__).resolve().parents[1]
base = ['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-v','ON_ERROR_STOP=1','-U','warehouse_admin']
# The fixed test-only database is created and removed within this script.
subprocess.run(base+['-d','postgres'],input='CREATE DATABASE warehouse_test;\n',text=True,check=True)
try:
    sql = 'GRANT CREATE ON DATABASE warehouse_test TO warehouse_owner; GRANT CONNECT,TEMPORARY ON DATABASE warehouse_test TO warehouse_loader; SET ROLE warehouse_owner;\n'
    sql += '\n'.join(path.read_text() for path in sorted((service/'sql').glob('[0-9][0-9][0-9]-*.sql')))
    subprocess.run(base+['-d','warehouse_test'],input=sql,text=True,check=True)
    subprocess.run(['docker','compose','run','--rm','--no-deps','--user','0:0','-e','PGDATABASE=warehouse_test',
        '-v','/srv/decom-sqlserver/secrets/warehouse/reader_password:/run/secrets/reader:ro',
        '-v','/srv/decom-sqlserver/secrets/warehouse/admin_password:/run/secrets/test_admin:ro',
        '-v',str(service/'tests')+':/tests:ro','loader','/bin/sh','-c',
        'pip install --disable-pip-version-check --no-cache-dir --target /tmp/test-deps pytest==9.0.2 >/dev/null && PYTHONPATH=/tmp/test-deps python -m pytest -p no:cacheprovider /tests -q'],cwd=service,check=True)
finally:
    subprocess.run(base+['-d','postgres'],input='DROP DATABASE warehouse_test WITH (FORCE);\n',text=True,check=True)
