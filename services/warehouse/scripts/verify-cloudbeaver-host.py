"""Verify saved PostgreSQL access through the existing CloudBeaver UI backend."""
import argparse
import hashlib
import http.cookiejar
import json
from pathlib import Path
import time
import urllib.request

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--initialize-connection',action='store_true')
parser.add_argument('--drivers',action='store_true')
parser.add_argument('--sales',action='store_true')
parser.add_argument('--order-updates',action='store_true')
args=parser.parse_args()
password=Path('/srv/decom-sqlserver/secrets/cloudbeaver/admin_password').read_text().strip()
opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def api(query,variables=None):
    request=urllib.request.Request('http://127.0.0.1:8978/api/gql',data=json.dumps({'query':query,'variables':variables or {}}).encode(),
        headers={'Content-Type':'application/json','Origin':'http://localhost:8978'})
    with opener.open(request,timeout=90) as response:
        result=json.load(response)
    if result.get('errors'):
        raise RuntimeError('; '.join(e['message'] for e in result['errors']).replace(password,'[REDACTED]'))
    return result['data']


api('mutation {openSession{valid}}')
try:
    login=api('query($c:Object!){authLogin(provider:"local",credentials:$c){userTokens{userId}}}',
        {'c':{'user':'stefen','password':hashlib.md5(password.encode(),usedforsecurity=False).hexdigest().upper()}})
    assert login['authLogin']['userTokens'][0]['userId']=='stefen'
    if args.drivers:
        print(json.dumps([d for d in api('{driverList{id name}}')['driverList'] if 'postgres' in (d['name'] or '').lower()]))
    else:
        project=next(p['id'] for p in api('{listProjects{id name}}')['listProjects'] if 'GlobalConfiguration' in p['id'])
        variables={'p':project,'c':'warehouse-postgres'}
        if args.initialize_connection:
            reader=Path('/srv/decom-sqlserver/secrets/warehouse/reader_password').read_text().strip()
            api('mutation($p:ID!,$c:ID!,$a:Object!){initConnection(projectId:$p,id:$c,credentials:$a,saveCredentials:true,sharedCredentials:true){id}}',
                {**variables,'a':{'userName':'warehouse_reader','userPassword':reader}})
            api('mutation($p:ID!,$c:ID!){closeConnection(projectId:$p,id:$c){id}}',variables)
        connection=api('mutation($p:ID!,$c:ID!){initConnection(projectId:$p,id:$c){connected}}',variables)
        assert connection['initConnection']['connected']
        variables['ctx']=api('mutation($p:ID!,$c:ID!){sqlContextCreate(projectId:$p,connectionId:$c){id}}',variables)['sqlContextCreate']['id']
        variables['sql']="SELECT (SELECT count(*) FROM mart.customers) AS customers,(SELECT count(*) FROM mart.orders) AS orders,has_table_privilege(current_user,'dv.h_customer','INSERT')::int AS can_insert;"
        if args.sales:
            variables['sql']="SELECT (SELECT count(*) FROM mart.customers),(SELECT count(*) FROM mart.orders),(SELECT count(*) FROM mart.order_lines),(SELECT count(*) FROM mart.invoices),(SELECT count(*) FROM mart.invoice_lines),(SELECT count(*) FROM mart.order_details),(SELECT count(*) FROM mart.invoice_details),has_table_privilege(current_user,'dv.s_invoice','INSERT')::int;"
        if args.order_updates:
            baseline=json.loads(Path('/srv/decom-sqlserver/validation/sql-objects-20261028/order-updates-results/manifest.json').read_text())
            case=baseline['cases']['upper_inclusive_100ns']
            literal=lambda value:"'"+value.replace("'","''")+"'"
            variables['sql']='SELECT count(*),has_table_privilege(current_user,\'ref.package_types\',\'INSERT\')::int FROM app.get_order_updates('+','.join(literal(v) for v in [case['last_cutoff'],case['new_cutoff'],baseline['reference_version']])+');'
        task=api('mutation($p:ID!,$c:ID!,$ctx:ID!,$sql:String!){asyncSqlExecuteQuery(projectId:$p,connectionId:$c,contextId:$ctx,sql:$sql){id running error{message}}}',variables)['asyncSqlExecuteQuery']
        for _ in range(240 if args.order_updates else 60):
            if not task['running']:
                break
            time.sleep(0.5)
            task=api('mutation($id:String!){asyncTaskInfo(id:$id,removeOnFinish:false){id running error{message}}}',{'id':task['id']})['asyncTaskInfo']
        assert not task['running'] and not task['error'],task
        rows=api('mutation($id:ID!){asyncSqlExecuteResults(taskId:$id){results{resultSet{rows}}}}',{'id':task['id']})['asyncSqlExecuteResults']['results'][0]['resultSet']['rows']
        expected=[663,73595,231412,70510,228265,231412,228265,0] if args.sales else [663,73595,0]
        if args.order_updates:
            expected=[case['rows'],0]
        assert [[int(v) for v in row] for row in rows]==[expected],rows
        print(json.dumps({'cloudbeaver_login':'passed','postgresql_saved_connection':'passed','validated_counts':expected[:-1],'reader_can_insert':False,
            **({'order_updates_api':'passed'} if args.order_updates else {})}))
        api('mutation($p:ID!,$c:ID!,$ctx:ID!){sqlContextDestroy(projectId:$p,connectionId:$c,contextId:$ctx)}',variables)
        api('mutation($p:ID!,$c:ID!){closeConnection(projectId:$p,id:$c){id}}',variables)
finally:
    api('mutation{closeSession}')
