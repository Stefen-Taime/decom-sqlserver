"""Apply checked-in SQL with a checksum ledger; passwords only pass through stdin."""
import hashlib
from pathlib import Path
import subprocess

secrets = Path('/srv/decom-sqlserver/secrets/warehouse')
passwords = {role:(secrets/filename).read_text().strip() for role,filename in
    [('warehouse_loader','loader_password'),('warehouse_reader','reader_password')]}


def sql(statement):
    result = subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],
        input=statement,text=True,capture_output=True)
    if result.returncode:
        message = result.stdout+result.stderr
        for password in passwords.values():
            message = message.replace(password,'[REDACTED]')
        raise RuntimeError(message)
    return result.stdout.strip()


provision = """BEGIN;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='warehouse_owner') THEN CREATE ROLE warehouse_owner NOLOGIN; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='warehouse_loader') THEN CREATE ROLE warehouse_loader LOGIN; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='warehouse_reader') THEN CREATE ROLE warehouse_reader LOGIN; END IF;
END $$;
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT CONNECT,TEMPORARY ON DATABASE warehouse TO warehouse_loader;
GRANT CONNECT ON DATABASE warehouse TO warehouse_reader;
GRANT CREATE ON DATABASE warehouse TO warehouse_owner;
"""
for role,password in passwords.items():
    provision += f"ALTER ROLE {role} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '{password.replace(chr(39),chr(39)*2)}';\n"
provision += "ALTER ROLE warehouse_reader SET default_transaction_read_only=on;\nCOMMIT;"
sql(provision)
for path in sorted(Path('sql').glob('[0-9][0-9][0-9]-*.sql')):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    has_ledger = sql("SELECT to_regclass('ops.schema_migrations') IS NOT NULL") == 't'
    previous = sql(f"SELECT sha256 FROM ops.schema_migrations WHERE version='{path.name}'") if has_ledger else ''
    if previous:
        if previous != digest:
            raise SystemExit('Applied migration was modified: '+path.name)
        print('Already applied: '+path.name)
        continue
    sql("BEGIN; SET ROLE warehouse_owner;\n"+path.read_text()+f"\nINSERT INTO ops.schema_migrations(version,sha256) VALUES('{path.name}','{digest}'); COMMIT;")
    print('Applied: '+path.name)
