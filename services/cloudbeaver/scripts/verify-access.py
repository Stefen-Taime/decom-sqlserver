"""Authenticate through CloudBeaver and query WWI without printing credentials."""
import http.cookiejar
import hashlib
import argparse
import json
from pathlib import Path
import time
import urllib.request

password = Path('/srv/decom-sqlserver/secrets/cloudbeaver/admin_password').read_text().strip()
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--initialize-connection', action='store_true')
parser.add_argument('--drivers', action='store_true', help='List available SQL Server driver identifiers')
args = parser.parse_args()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def api(query, variables=None):
    body = json.dumps({'query': query, 'variables': variables or {}}).encode()
    request = urllib.request.Request('http://127.0.0.1:8978/api/gql', data=body,
        headers={'Content-Type': 'application/json', 'Origin': 'http://localhost:8978'})
    with opener.open(request, timeout=60) as response:
        result = json.load(response)
    if result.get('errors'):
        raise RuntimeError('; '.join(e['message'] for e in result['errors']).replace(password, '[REDACTED]'))
    return result['data']


try:
    api('mutation { openSession { valid } }')
    authenticated = api('query($c:Object!) { authLogin(provider:"local",credentials:$c) { userTokens { userId } } }',
        # Match CloudBeaver's AuthProviderService client-side credential processing.
        {'c': {'user': 'stefen', 'password': hashlib.md5(password.encode(), usedforsecurity=False).hexdigest().upper()}})
    assert authenticated['authLogin']['userTokens'][0]['userId'] == 'stefen', authenticated
    print('CloudBeaver login: PASS (stefen)', flush=True)
    if args.drivers:
        print(json.dumps([d for d in api('{driverList{id name}}')['driverList'] if 'sql server' in (d['name'] or '').lower()]))
        raise SystemExit(0)
    projects = api('{listProjects{id name}}')['listProjects']
    project = next(p['id'] for p in projects if 'GlobalConfiguration' in p['id'])
    variables = {'p': project, 'c': 'wwi-sqlserver'}
    if args.initialize_connection:
        credentials = {'userName': 'lab_reader', 'userPassword': Path('/srv/decom-sqlserver/secrets/cloudbeaver/sql_reader_password').read_text().strip()}
        api('mutation($p:ID!,$c:ID!,$a:Object!){initConnection(projectId:$p,id:$c,credentials:$a,saveCredentials:true,sharedCredentials:true){id}}', {**variables, 'a':credentials})
        api('mutation($p:ID!,$c:ID!){closeConnection(projectId:$p,id:$c){id}}', variables)
    connection = api('mutation($p:ID!,$c:ID!){initConnection(projectId:$p,id:$c){id name connected}}', variables)
    assert connection['initConnection']['connected'], connection
    print('SQL Server saved connection: PASS', flush=True)
    context = api('mutation($p:ID!,$c:ID!){sqlContextCreate(projectId:$p,connectionId:$c){id}}', variables)
    variables['ctx'] = context['sqlContextCreate']['id']
    variables['sql'] = "SELECT (SELECT COUNT(*) FROM Sales.Customers) AS Customers, (SELECT COUNT(*) FROM Sales.Orders) AS Orders, (SELECT COUNT(*) FROM Sales.OrderLines) AS OrderLines, (SELECT COUNT(*) FROM sys.procedures WHERE is_ms_shipped=0 AND schema_id<>SCHEMA_ID('cdc')) AS StoredProcedures, HAS_PERMS_BY_NAME('Sales.Customers','OBJECT','UPDATE') AS CanUpdateCustomers;"
    task = api('mutation($p:ID!,$c:ID!,$ctx:ID!,$sql:String!){asyncSqlExecuteQuery(projectId:$p,connectionId:$c,contextId:$ctx,sql:$sql){id running taskResult error{message}}}', variables)['asyncSqlExecuteQuery']
    for _ in range(60):
        if not task['running']:
            break
        time.sleep(0.5)
        task = api('mutation($id:String!){asyncTaskInfo(id:$id,removeOnFinish:false){id running taskResult error{message}}}', {'id':task['id']})['asyncTaskInfo']
    assert not task['running'] and not task['error'], task
    results = api('mutation($id:ID!){asyncSqlExecuteResults(taskId:$id){results{resultSet{rows}}}}', {'id':task['id']})
    rows = results['asyncSqlExecuteResults']['results'][0]['resultSet']['rows']
    assert [[int(value) for value in row] for row in rows] == [[663, 73595, 231412, 42, 0]], rows
    print('SQL query: PASS (663 customers, 73595 orders, 231412 order lines, 42 procedures, UPDATE permission=0)', flush=True)
    api('mutation($p:ID!,$c:ID!,$ctx:ID!){sqlContextDestroy(projectId:$p,connectionId:$c,contextId:$ctx)}', variables)
    api('{authLogout}')
finally:
    api('mutation { closeSession }')
