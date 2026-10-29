"""Live SQL Server -> archive -> warehouse probe, durable restart and replay checks."""
import json
from pathlib import Path
import subprocess
import time

service=Path(__file__).resolve().parents[1]


def compose(*args):
    return subprocess.run(['docker','compose',*args],cwd=service,text=True,capture_output=True,check=True).stdout


def query(sql):
    output=subprocess.check_output(['docker','exec','decom-warehouse-postgres-1','psql','-X','-A','-t','-v','ON_ERROR_STOP=1',
        '-U','warehouse_admin','-d','warehouse','-c',sql],text=True)
    return json.loads(output)


def counts():
    return query("""SELECT json_build_object(
        'customers',(SELECT count(*) FROM mart.customers),'orders',(SELECT count(*) FROM mart.orders),
        'journal_events',(SELECT count(*) FROM staging.cdc_event),
        'customer_hubs',(SELECT count(*) FROM dv.h_customer),'order_hubs',(SELECT count(*) FROM dv.h_order),
        'customer_satellites',(SELECT count(*) FROM dv.s_customer),'order_satellites',(SELECT count(*) FROM dv.s_order),
        'relationships',(SELECT count(*) FROM dv.l_order_customer),'batches',(SELECT count(*) FROM ops.archive_batches));""")


probe=json.loads(subprocess.check_output(['python3',str(service/'scripts/probe-host.py')],text=True))
marker=probe['marker']
for _ in range(90):
    events=query(f"""SELECT coalesce(json_agg(json_build_object('op',operation,'value',row_data->>'Comments',
        'commit_lsn',encode(commit_lsn,'hex'),'change_lsn',encode(change_lsn,'hex'),'serial',event_serial_no)
        ORDER BY commit_lsn,change_lsn,event_serial_no),'[]'::json)
        FROM staging.cdc_event WHERE table_name='Sales.Orders' AND source_pk={probe['order_id']}
        AND row_data->>'Comments' LIKE '{marker}%';""")
    if [e['op'] for e in events]==['c','u','d']:
        break
    time.sleep(2)
else:
    raise RuntimeError('Live CDC probe did not reach the warehouse within 180 seconds')
assert [e['value'] for e in events]==[marker+'-before',marker+'-after',marker+'-after']
assert query(f"SELECT count(*) FROM mart.orders WHERE order_id={probe['order_id']}")==0
print('Live insert/update/delete, retained history and current deletion: PASS',flush=True)
before=counts()
assert before['customers']==663 and before['orders']==73595,before
compose('stop','loader')
try:
    compose('up','-d','--no-deps','--force-recreate','--wait','--wait-timeout','180','postgres')
    after_restart=counts()
    assert after_restart==before,(before,after_restart)
    print('PostgreSQL container recreation retained all state: PASS',flush=True)
    replay_output=compose('run','--rm','--no-deps','loader','python','load.py','--replay')
    replay=json.loads(replay_output.strip().splitlines()[-1])
    assert replay['new_events']==0,replay
    after_replay=counts()
    assert after_replay==before,(before,after_replay)
    print('Full archive replay created no duplicate events or vault rows: PASS',flush=True)
    result={'live_probe':probe,'source_events':events,'counts_before_restart':before,'counts_after_restart':after_restart,
        'counts_after_replay':after_replay,'replay':replay,'current_probe_row_absent':True,'purge_authorized':False}
    path=Path('/srv/decom-sqlserver/validation/warehouse-live-recovery.json')
    path.write_text(json.dumps(result,indent=2)+'\n')
    path.chmod(0o600)
    print(json.dumps(result,indent=2))
finally:
    compose('up','-d','--wait','--wait-timeout','360','loader')
