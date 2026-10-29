"""Dedicated metadata database and SSH identity restricted to the root-owned job runner."""
import base64
import json
import os
from pathlib import Path
import pwd
import secrets
import shutil
import subprocess
import tempfile

root=Path('/srv/decom-sqlserver');secret_dir=root/'secrets/airflow';data=root/'airflow'
if not secret_dir.exists():
    if data.exists() and any(data.rglob('*')):
        raise SystemExit('Existing Airflow state without secrets; restore them before continuing.')
    temporary=Path(tempfile.mkdtemp(prefix='.airflow-',dir=secret_dir.parent))
    try:
        password=secrets.token_hex(32)
        (temporary/'metadata_password').write_text(password+'\n')
        (temporary/'sql_connection').write_text('postgresql+psycopg2://airflow_meta:'+password+'@postgres:5432/airflow_meta?application_name=airflow\n')
        (temporary/'fernet_key').write_text(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()+'\n')
        for name in ['api_secret','jwt_secret']:
            (temporary/name).write_text(secrets.token_hex(48)+'\n')
        (temporary/'admin.json').write_text(json.dumps({'username':'stefen','password':secrets.token_hex(32)})+'\n')
        subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-C','decom-airflow-runner','-f',str(temporary/'runner_ed25519')],check=True)
        public=Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().strip().split()
        (temporary/'runner_known_hosts').write_text('lab-host '+public[0]+' '+public[1]+'\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists(): shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for name in ['metadata_password','sql_connection','fernet_key','api_secret','jwt_secret','admin.json','runner_ed25519','runner_ed25519.pub','runner_known_hosts']:
    path=secret_dir/name
    if not path.is_file() or not path.stat().st_size: raise SystemExit('Incomplete Airflow secrets; restore missing files.')
    os.chown(path,0 if name=='metadata_password' else 50000,0);path.chmod(0o400)
for name,uid,gid,mode in [('logs',50000,0,0o770),('runner',0,0,0o700),('runner/requests',0,0,0o700),('runner/logs',0,0,0o700)]:
    path=data/name;path.mkdir(parents=True,exist_ok=True);os.chown(path,uid,gid);path.chmod(mode)

password=(secret_dir/'metadata_password').read_text().strip()
def pg(statement,database='postgres'):
    result=subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-A','-t',
        '-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d',database],input=statement,text=True,capture_output=True)
    if result.returncode: raise RuntimeError((result.stdout+result.stderr).replace(password,'[REDACTED]'))
    return result.stdout.strip()
pg("DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='airflow_meta') THEN CREATE ROLE airflow_meta LOGIN; END IF; END $$;\n"
   +"ALTER ROLE airflow_meta NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 16 PASSWORD '"+password+"';")
if pg("SELECT 1 FROM pg_database WHERE datname='airflow_meta'")!='1':
    pg('CREATE DATABASE airflow_meta OWNER airflow_meta;')
assert pg("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='airflow_meta'")=='airflow_meta'
pg('REVOKE ALL ON DATABASE airflow_meta FROM PUBLIC; GRANT CONNECT ON DATABASE airflow_meta TO airflow_meta;')
pg('REVOKE ALL ON SCHEMA public FROM PUBLIC; GRANT ALL ON SCHEMA public TO airflow_meta;','airflow_meta')

home=Path('/var/lib/decom-airflow-runner')
try:
    account=pwd.getpwnam('airflow-runner')
    if account.pw_dir!=str(home): raise SystemExit('Existing runner account has an unexpected home.')
except KeyError:
    subprocess.run(['useradd','--system','--create-home','--home-dir',str(home),'--shell','/bin/sh','airflow-runner'],check=True)
os.chown(home,0,0);home.chmod(0o755)
ssh=home/'.ssh';ssh.mkdir(exist_ok=True);os.chown(ssh,0,0);ssh.chmod(0o755)
authorized=ssh/'authorized_keys'
authorized.write_text('restrict,command="/usr/bin/sudo -n /usr/local/libexec/decom-airflow-runner" '+(secret_dir/'runner_ed25519.pub').read_text())
os.chown(authorized,0,0);authorized.chmod(0o644)
runner=Path('/usr/local/libexec/decom-airflow-runner');runner.parent.mkdir(parents=True,exist_ok=True)
shutil.copyfile('runner/runner.py',runner);os.chown(runner,0,0);runner.chmod(0o755)
sudoers=Path('/etc/sudoers.d/decom-airflow-runner')
sudoers.write_text('airflow-runner ALL=(root) NOPASSWD: /usr/local/libexec/decom-airflow-runner ""\n')
sudoers.chmod(0o440)
subprocess.run(['visudo','-cf',str(sudoers)],check=True,stdout=subprocess.DEVNULL)
Path('.env').write_text(f'AIRFLOW_DATA_DIR={data}\nAIRFLOW_SECRET_DIR={secret_dir}\n');Path('.env').chmod(0o600)
print('Airflow metadata database, immutable secrets and restricted runner identity prepared.')
