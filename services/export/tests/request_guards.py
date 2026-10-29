"""Request serialization and contract drift checks using the completed order export."""
import copy
import fcntl
import json
import os
from pathlib import Path
import subprocess
from common import atomic_json, load_contract, require
from export import run

directory=Path(os.environ['EXPORT_STATE_DIR'])
report=json.loads((directory/'validation-report.json').read_text())
request=report['exports']['order_details']['request_id']
lock_path=directory/f'order_details-v1-{request}.lock'
with lock_path.open('a') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    result=subprocess.run(['python','export.py','--dataset','order_details','--request-id',request],
                          text=True,capture_output=True)
    require(result.returncode==1 and 'already running' in result.stderr,'Concurrent request was accepted')
contract=copy.deepcopy(load_contract('order_details'))
contract['rows_per_file']=25000
try:
    run(contract,request)
except ValueError as exc:
    require('contract changed' in str(exc),'Unexpected contract rejection')
else:
    raise AssertionError('Contract drift accepted for the same request')
for check in ['same request cannot run concurrently on this host','same request cannot change its contract']:
    if check not in report['checks']:
        report['checks'].append(check)
atomic_json(directory/'validation-report.json',report)
print(json.dumps({'checks':report['checks'][-2:]}))
