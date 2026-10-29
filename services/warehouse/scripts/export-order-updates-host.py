"""Execute the real source procedure and stream its result sets to protected JSONL."""
import argparse
from datetime import date,datetime,timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

service=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('source_export',service/'scripts/source-export.py')
source=importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':'))


def tick(value,delta=0):
    core,_,fraction=value.partition('.')
    dt=datetime.fromisoformat(core)
    ticks=((dt.toordinal()*86400+dt.hour*3600+dt.minute*60+dt.second)*10000000+int(fraction.ljust(7,'0')))+delta
    day,remainder=divmod(ticks,86400*10000000)
    seconds,fraction=divmod(remainder,10000000)
    hour,seconds=divmod(seconds,3600)
    minute,second=divmod(seconds,60)
    return f'{date.fromordinal(day).isoformat()}T{hour:02}:{minute:02}:{second:02}.{fraction:07}'


def source_state():
    return source.query("SELECT OBJECT_DEFINITION(OBJECT_ID(N'Integration.GetOrderUpdates')) AS definition,JSON_QUERY((SELECT PackageTypeID,PackageTypeName,ValidFrom,ValidTo FROM Warehouse.PackageTypes ORDER BY PackageTypeID FOR JSON PATH,INCLUDE_NULL_VALUES)) AS packages FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory-dir',required=True)
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    os.umask(0o077)
    inventory_dir=Path(args.inventory_dir)
    directory=Path(args.output_dir)
    directory.mkdir(parents=True,exist_ok=False,mode=0o700)
    catalog=json.loads((inventory_dir/'catalog.json').read_text())
    metadata=json.loads((inventory_dir/'metadata.json').read_text())
    reference=json.loads((inventory_dir/'package-types.json').read_text())
    definition_sha=next(p['definition_sha256'] for p in catalog['procedures'] if p['source_object']=='Integration.GetOrderUpdates')
    before=source_state()
    if hashlib.sha256(before['definition'].encode()).hexdigest()!=definition_sha or hashlib.sha256(canonical(before['packages']).encode()).hexdigest()!=reference['sha256']:
        raise ValueError('Source procedure or reference changed since inventory')
    boundary=source.query("SELECT MAX(CASE WHEN ol.LastEditedWhen>o.LastEditedWhen THEN ol.LastEditedWhen ELSE o.LastEditedWhen END) AS point FROM Sales.Orders o JOIN Sales.OrderLines ol ON ol.OrderID=o.OrderID FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")['point']
    point=tick(boundary)
    cases=[('full_current','0001-01-01','9999-12-31T23:59:59.9999999'),
        ('upper_inclusive_100ns',tick(point,-1),point),('lower_exclusive_100ns',point,tick(point,1)),
        ('equal_bounds',point,point),('reversed_bounds',point,tick(point,-1)),('null_lower',None,point),('null_upper',point,None)]
    columns=metadata['pilot_result_columns']
    if len(columns)!=18 or any(c['error_number'] is not None for c in columns):
        raise ValueError('Source result metadata is not the reviewed 18-column contract')
    identifier=lambda value:'['+value.replace(']',']]')+']'
    declarations=[]
    for column in columns:
        if not re.fullmatch(r'(date|int|datetime2\(7\)|nvarchar\((50|100)\)|(?:numeric|decimal)\((18,2|18,3|29,2|38,6)\))',column['system_type_name']):
            raise ValueError('Unexpected source result type')
        declarations.append(identifier(column['name'])+' '+column['system_type_name']+' NULL')
    names=','.join(identifier(c['name']) for c in columns)
    projection=','.join('r.'+identifier(c['name']) for c in columns)
    sql='SET NOCOUNT ON; SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;\n'
    sql+='CREATE TABLE #actual ('+','.join(declarations)+',_ordinal bigint IDENTITY(1,1));\nDECLARE @last datetime2(7),@new datetime2(7),@status int;\n'
    literal=lambda value:'NULL' if value is None else "CONVERT(datetime2(7),N'"+value.replace("'","''")+"',126)"
    for name,lower,upper in cases:
        sql+=f"TRUNCATE TABLE #actual; SET @last={literal(lower)}; SET @new={literal(upper)};\n"
        sql+=f"INSERT #actual({names}) EXEC @status=Integration.GetOrderUpdates @LastCutoff=@last,@NewCutoff=@new;\n"
        sql+="IF @status<>0 THROW 51000,'Source procedure returned a nonzero status',1;\n"
        sql+=f"PRINT 'CASE:{name}'; SELECT (SELECT {projection} FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES) AS row_json FROM #actual r ORDER BY r.[WWI Order ID],r._ordinal;\n"
    sql+="COMMIT; PRINT 'EXPORT_COMMITTED';\nGO\n"
    command='export SQLCMDPASSWORD="$(cat /run/secrets/sa_password)"; exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -d WideWorldImporters -C -b -l 10 -y 0 -w 65535'
    result={'captured_at':datetime.now(timezone.utc).isoformat(),'source_procedure':'Integration.GetOrderUpdates','definition_sha256':definition_sha,
        'reference_version':reference['version'],'reference_sha256':reference['sha256'],'columns':columns,'cases':{},
        'isolation':'SERIALIZABLE, all source procedure cases in one transaction; quiet fixture validation','source_return_codes':0}
    with tempfile.TemporaryFile(mode='w+') as error:
        process=subprocess.Popen(['docker','exec','-i','decom-sqlserver-sqlserver-1','/bin/bash','-c',command],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=error,text=True,encoding='utf-8')
        process.stdin.write(sql); process.stdin.close()
        output=None; case=None; committed=False
        try:
            for line in process.stdout:
                line=line.rstrip('\r\n')
                if line.startswith('CASE:'):
                    if output:
                        output.close(); result['cases'][case]['file_sha256']=hasher.hexdigest()
                    case=line.removeprefix('CASE:')
                    if case in result['cases'] or case not in [c[0] for c in cases]: raise ValueError('Unexpected case boundary')
                    _,lower,upper=next(c for c in cases if c[0]==case)
                    result['cases'][case]={'last_cutoff':lower,'new_cutoff':upper,'file':case+'.jsonl','rows':0}
                    output=(directory/(case+'.jsonl')).open('xb'); hasher=hashlib.sha256()
                elif line=='EXPORT_COMMITTED': committed=True
                elif not line.strip() or line.strip()=='row_json' or set(line.strip())=={'-'}: continue
                else:
                    try: row=json.loads(line,parse_float=Decimal)
                    except Exception: raise ValueError('Source output is not one complete JSON object per row') from None
                    if case is None or set(row)!=set(c['name'] for c in columns): raise ValueError('Source output shape changed')
                    encoded=(line+'\n').encode(); output.write(encoded); hasher.update(encoded)
                    result['cases'][case]['rows']+=1
            if process.wait()!=0 or not committed or len(result['cases'])!=len(cases):
                raise RuntimeError('Source procedure export did not commit')
            output.close(); result['cases'][case]['file_sha256']=hasher.hexdigest()
            if source_state()!=before: raise ValueError('Procedure or reference changed during validation')
            (directory/'manifest.json').write_text(json.dumps(result,indent=2)+'\n')
            print(json.dumps({'source_results':str(directory),'rows':{k:v['rows'] for k,v in result['cases'].items()}}))
        finally:
            if output: output.close()
            if process.poll() is None: process.terminate(); process.wait()


if __name__=='__main__': main()
