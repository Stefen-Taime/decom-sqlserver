"""Host provisioning: hashed broker definitions, per-service secrets and scoped S3 identities."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import tempfile

root=Path('/srv/decom-sqlserver')
secret_dir=root/'secrets/messaging';data=root/'messaging'
spec=importlib.util.spec_from_file_location('topology',Path('app/topology.py'))
topology=importlib.util.module_from_spec(spec);spec.loader.exec_module(topology)
config=root/'secrets/cdc/seaweed-s3.json'
s3=json.loads(config.read_text())
if not secret_dir.exists():
    if (data.exists() and any(data.rglob('*'))) or any(i['name'].startswith('invoice-') for i in s3['identities']):
        raise SystemExit('Messaging state exists without its secrets; restore before proceeding.')
    temporary=Path(tempfile.mkdtemp(prefix='.messaging-',dir=secret_dir.parent))
    try:
        users=[]
        for filename,username,tags in [('admin','stefen',['administrator']),('publisher','export_publisher',[]),('invoice-app','invoice_app',[])]:
            password=secrets.token_hex(32);salt=secrets.token_bytes(4)
            (temporary/(filename+'.json')).write_text(json.dumps({'username':username,'password':password})+'\n')
            users.append({'name':username,'password_hash':base64.b64encode(salt+hashlib.sha256(salt+password.encode()).digest()).decode(),
                          'hashing_algorithm':'rabbit_password_hashing_sha256','tags':tags,'limits':{}})
        defs=topology.definitions();defs.update(users=users,permissions=topology.permissions())
        (temporary/'definitions.json').write_text(json.dumps(defs)+'\n')
        for name in ['publisher','app','test-reader']:
            (temporary/(name+'-s3.json')).write_text(json.dumps({'access_key':'invoice-'+name+'-'+secrets.token_hex(12),
                                                               'secret_key':secrets.token_hex(32)})+'\n')
        temporary.rename(secret_dir)
    finally:
        if temporary.exists(): shutil.rmtree(temporary)
secret_dir.chmod(0o700)
for name,uid in [('definitions.json',999),('admin.json',0),('publisher.json',10008),('invoice-app.json',10009),
                 ('publisher-s3.json',10008),('app-s3.json',10009),('test-reader-s3.json',0)]:
    path=secret_dir/name
    if not path.is_file() or not path.stat().st_size: raise SystemExit('Incomplete messaging secrets; restore them.')
    os.chown(path,uid,uid);path.chmod(0o400)
for name,uid in [('broker',999),('dispatcher',10008),('app',10009),('validation',0)]:
    path=data/name;path.mkdir(parents=True,exist_ok=True);os.chown(path,uid,uid);path.chmod(0o700)
changed=False
for name,actions in [
    ('publisher',['List:warehouse-exports','Read:warehouse-exports/published/dataset=invoice_details/*']),
    ('app',['Read:warehouse-exports/published/dataset=invoice_details/*','Read:warehouse-exports/datasets/dataset=invoice_details/*']),
    ('test-reader',['Read:warehouse-exports/validation/rabbitmq-v1/*','List:warehouse-exports'])]:
    values=json.loads((secret_dir/(name+'-s3.json')).read_text())
    identity={'name':'invoice-'+name,'credentials':[{'accessKey':values['access_key'],'secretKey':values['secret_key']}],'actions':actions}
    previous=next((i for i in s3['identities'] if i['name']==identity['name']),None)
    if previous and previous!=identity: raise SystemExit('S3 messaging identity drift; review before proceeding.')
    if not previous: s3['identities'].append(identity);changed=True
if changed:
    backup=config.with_name('seaweed-s3.pre-messaging.json')
    if not backup.exists(): shutil.copy2(config,backup);backup.chmod(0o400)
    with config.open('w') as stream:
        stream.write(json.dumps(s3)+'\n');stream.flush();os.fsync(stream.fileno())
    Path('.reload-seaweed').touch(mode=0o600)
Path('.env').write_text(f'MESSAGING_DATA_DIR={data}\nMESSAGING_SECRET_DIR={secret_dir}\n')
Path('.env').chmod(0o600)
print('Messaging secrets, hashed definitions and scoped S3 identities prepared.')
