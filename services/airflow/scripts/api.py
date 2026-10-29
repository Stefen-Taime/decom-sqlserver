"""Local operator API client; bootstrap password and JWT never enter command arguments."""
import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen

BASE = 'http://127.0.0.1:8080'


class Client:
    def __init__(self):
        credentials = json.loads(Path('/srv/decom-sqlserver/secrets/airflow/admin.json').read_text())
        request = Request(BASE + '/auth/token', data=json.dumps(credentials).encode(),
                          headers={'Content-Type': 'application/json'}, method='POST')
        with urlopen(request, timeout=30) as response:
            self.token = json.load(response)['access_token']

    def request(self, method, path, data=None):
        if not path.startswith('/') or path.startswith('//'):
            raise ValueError('Provide a local API path')
        request = Request(BASE + path, data=None if data is None else json.dumps(data).encode(),
                          headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'},
                          method=method)
        with urlopen(request, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path')
    parser.add_argument('--method', default='GET', choices=['GET', 'POST', 'PATCH'])
    parser.add_argument('--json-file', type=Path)
    args = parser.parse_args()
    value = Client().request(args.method, args.path,
                             json.loads(args.json_file.read_text()) if args.json_file else None)
    print(json.dumps(value, indent=2))
