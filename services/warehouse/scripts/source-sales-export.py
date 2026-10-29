"""Bounded-memory, read-only five-table source baseline for a quiet lab validation window.

SERIALIZABLE holds source read locks until all five scans finish. Do not use this
fixture validation protocol as a zero-downtime production cutover protocol.
"""
import argparse
from datetime import datetime,timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'loader'))
from sales_contract import TABLE_SPECS


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    os.umask(0o077)
    directory=Path(args.output_dir)
    directory.mkdir(parents=True,exist_ok=False,mode=0o700)
    sql='SET NOCOUNT ON; SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;\n'
    for table,spec in TABLE_SPECS.items():
        projection=','.join('t.['+c+']' for c in spec['columns'].split(','))
        sql+=f"PRINT 'BEGIN:{table}'; SELECT (SELECT {projection} FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES) AS row_json FROM {table} t ORDER BY t.[{spec['pk']}];\n"
    sql+="COMMIT; PRINT 'EXPORT_COMMITTED';\nGO\n"
    command='export SQLCMDPASSWORD="$(cat /run/secrets/sa_password)"; exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -d WideWorldImporters -C -b -l 10 -y 0 -w 65535'
    result={'captured_at':datetime.now(timezone.utc).isoformat(),'isolation':'SERIALIZABLE; five scans in one read-only source transaction', 'tables':{}}
    with tempfile.TemporaryFile(mode='w+') as error:
        process=subprocess.Popen(['docker','exec','-i','decom-sqlserver-sqlserver-1','/bin/bash','-c',command],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=error,text=True,encoding='utf-8')
        process.stdin.write(sql)
        process.stdin.close()
        output=None
        committed=False
        try:
            table=None
            for line in process.stdout:
                line=line.rstrip('\r\n')
                if line.startswith('BEGIN:'):
                    if output:
                        output.close()
                        result['tables'][table]['file_sha256']=hasher.hexdigest()
                    table=line.removeprefix('BEGIN:')
                    if table not in TABLE_SPECS or table in result['tables']:
                        raise ValueError('Unexpected table boundary')
                    spec=TABLE_SPECS[table]
                    result['tables'][table]={'file':spec['view']+'.jsonl','columns':spec['columns'].split(','),'rows':0}
                    output=(directory/(spec['view']+'.jsonl')).open('xb')
                    hasher=hashlib.sha256()
                    last_key=-1
                elif line=='EXPORT_COMMITTED':
                    committed=True
                elif not line.strip() or line.strip()=='row_json' or set(line.strip())=={'-'}:
                    continue
                else:
                    # One SQL scalar per row. A wrapped/truncated line fails closed.
                    try:
                        row=json.loads(line,parse_float=Decimal)
                    except Exception:
                        raise ValueError('Source output is not one complete JSON object per row; no baseline published') from None
                    if table is None or set(row)!=set(result['tables'][table]['columns']) or row[spec['pk']]<=last_key:
                        raise ValueError('Source keys or projection are inconsistent')
                    last_key=row[spec['pk']]
                    encoded=(line+'\n').encode()
                    output.write(encoded)
                    hasher.update(encoded)
                    result['tables'][table]['rows']+=1
            if process.wait()!=0 or not committed or set(result['tables'])!=set(TABLE_SPECS):
                raise RuntimeError('Source transaction did not complete; no baseline published')
            output.close()
            result['tables'][table]['file_sha256']=hasher.hexdigest()
            (directory/'manifest.json').write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps({'source_export':str(directory),'rows':{t:v['rows'] for t,v in result['tables'].items()}}))
        finally:
            if output:
                output.close()
            if process.poll() is None:
                process.terminate()
                process.wait()


if __name__=='__main__':
    main()
