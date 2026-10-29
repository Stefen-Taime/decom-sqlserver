"""Protected VM observations of connector files and a recent local resume exercise."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path('/srv/decom-sqlserver')
SERVICE=ROOT/'workspace/services/cdc'
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from offset_checkpoint import decode


def inspect(name):
    # Do not return Config.Env: it can contain credentials.
    return json.loads(subprocess.check_output(['docker','inspect','--format',
        '{"id":{{json .Id}},"image":{{json .Image}},"mounts":{{json .Mounts}},"state":{{json .State}}}',name],text=True))


def state(flush=False):
    info=inspect('decom-cdc-debezium-1');directory=ROOT/'cdc/debezium'
    mount=[m for m in info['mounts'] if m['Destination']=='/debezium/data']
    if len(mount)!=1 or mount[0]['Type']!='bind' or Path(mount[0]['Source'])!=directory:
        raise ValueError('Connector state is not on the expected persistent bind mount')
    if flush and info['state']['Running']: raise ValueError('Flush/copy requires a stopped connector')
    config=SERVICE/'debezium/application.properties'
    mounted=[m for m in info['mounts'] if m['Destination']=='/debezium/config/application.properties']
    if len(mounted)!=1 or Path(mounted[0]['Source'])!=config or mounted[0]['RW']:
        raise ValueError('Connector configuration mount differs')
    data={}
    for name,maximum in [('offsets.dat',1024*1024),('schema-history.dat',64*1024*1024)]:
        path=directory/name
        if path.is_symlink() or not 0<path.stat().st_size<=maximum: raise ValueError('Checkpoint file bounds invalid')
        with path.open('rb') as stream:
            data[name]=stream.read(maximum+1)
            if flush: os.fsync(stream.fileno())
        if len(data[name])>maximum: raise ValueError('Checkpoint file grew beyond its bound')
    if flush:
        descriptor=os.open(directory,os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    observation={'container_id':info['id'],'image_id':info['image'],
        'config_sha256':hashlib.sha256(config.read_bytes()).hexdigest(),
        'state_directory':str(directory),'checkpoint':decode(data['offsets.dat']),
        'files':{name:{'sha256':hashlib.sha256(value).hexdigest(),'bytes':len(value)} for name,value in data.items()},
        'source_container_id':inspect('decom-sqlserver-sqlserver-1')['id']}
    return observation,data


def stable_state():
    for _ in range(10):
        try:
            first,_=state();time.sleep(1);second,data=state()
            if first==second: return second,data
        except (ValueError,FileNotFoundError): pass
    raise ValueError('No stable readable checkpoint sample was obtained')


def collect_checkpoint():
    current,_=stable_state()
    result={'contract':'cdc-checkpoint-audit-input/v1','observed_at':datetime.now(timezone.utc).isoformat(),
            'current':current,'exercise':None,'exercise_sha256':None}
    pointer=ROOT/'validation/cdc-resume-v1-latest.json'
    if pointer.exists():
        link=json.loads(pointer.read_text());path=Path(link['report']).resolve()
        if not path.is_relative_to(ROOT/'validation') or path.name!='report.json':
            raise ValueError('Resume report pointer escapes its evidence directory')
        data=path.read_bytes()
        if hashlib.sha256(data).hexdigest()!=link['sha256']: raise ValueError('Resume report hash differs')
        result['exercise']=json.loads(data);result['exercise_sha256']=link['sha256']
    return result
