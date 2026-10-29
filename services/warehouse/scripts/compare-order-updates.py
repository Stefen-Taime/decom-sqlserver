"""Read-only, duplicate-preserving comparison of real source results with the target API."""
import argparse
from collections import Counter
from datetime import date,datetime,timezone
from decimal import Decimal
import hashlib
from itertools import groupby
import json
from pathlib import Path
import sys
import time

sys.path.insert(0,'/app')
from load import connect

MONEY={'Unit Price','Tax Rate','Total Excluding Tax','Tax Amount','Total Including Tax'}


def normalize(row):
    result={}
    for name,value in row.items():
        if isinstance(value,(date,datetime)): value=value.isoformat()
        if value is not None and name in MONEY:
            value=format(Decimal(str(value)),'f')
            if '.' in value: value=value.rstrip('0').rstrip('.')
            if Decimal(value)==0: value='0'
        if value is not None and name=='Last Modified When':
            core,_,fraction=value.partition('.')
            value=core+'.'+fraction.ljust(7,'0')
        result[name]=value
    return json.dumps(result,sort_keys=True,ensure_ascii=False,separators=(',',':'))


def grouped(rows):
    previous=-1
    for key,items in groupby(rows,key=lambda r:r['WWI Order ID']):
        if key<previous: raise ValueError('Result order is not monotonic by OrderID')
        previous=key
        counts=Counter()
        for item in items:
            counts[normalize(item)]+=1
            if sum(counts.values())>10000: raise ValueError('Order group exceeds the bounded validation contract')
        yield key,counts


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source_directory')
    args=parser.parse_args()
    directory=Path(args.source_directory)
    manifest=json.loads((directory/'manifest.json').read_text())
    expected_columns=[c['name'] for c in manifest['columns']]
    reports={}
    with connect() as conn,conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout='300s'")
        assert conn.execute('SELECT current_user').fetchone()[0]=='warehouse_reader'
        for name,case in manifest['cases'].items():
            started=time.monotonic()
            source_count=target_count=missing=extra=0
            source_hash,target_hash,file_hash=hashlib.sha256(),hashlib.sha256(),hashlib.sha256()
            def source_rows():
                with (directory/case['file']).open('rb') as f:
                    for line in f:
                        file_hash.update(line)
                        yield json.loads(line,parse_float=Decimal)
            with conn.cursor(name='order_updates_comparison') as cursor:
                cursor.itersize=1000
                cursor.execute('SELECT * FROM app.get_order_updates(%s,%s,%s)',(case['last_cutoff'],case['new_cutoff'],manifest['reference_version']))
                names=[c.name for c in cursor.description]
                if names!=expected_columns: raise ValueError('Target result column names/order differ')
                target_rows=(dict(zip(names,row)) for row in cursor)
                left,right=iter(grouped(source_rows())),iter(grouped(target_rows))
                a,b=next(left,None),next(right,None)
                while a is not None or b is not None:
                    take_a=b is None or (a is not None and a[0]<=b[0])
                    take_b=a is None or (b is not None and b[0]<=a[0])
                    ac=a[1] if take_a else Counter(); bc=b[1] if take_b else Counter()
                    missing+=sum((ac-bc).values()); extra+=sum((bc-ac).values())
                    if take_a:
                        source_count+=sum(ac.values())
                        for value,count in sorted(ac.items()): source_hash.update(((value+'\n')*count).encode())
                        a=next(left,None)
                    if take_b:
                        target_count+=sum(bc.values())
                        for value,count in sorted(bc.items()): target_hash.update(((value+'\n')*count).encode())
                        b=next(right,None)
            if file_hash.hexdigest()!=case['file_sha256'] or source_count!=case['rows']:
                raise ValueError('Source result file checksum/count mismatch')
            reports[name]={'source_rows':source_count,'target_rows':target_count,'missing_or_different_rows':missing,'extra_or_different_rows':extra,
                'source_sha256':source_hash.hexdigest(),'target_sha256':target_hash.hexdigest(),'elapsed_seconds':round(time.monotonic()-started,2),
                'last_cutoff':case['last_cutoff'],'new_cutoff':case['new_cutoff']}
            print(json.dumps({'case':name,'rows':target_count,'missing':missing,'extra':extra}),file=sys.stderr,flush=True)
        privileges=conn.execute("SELECT has_function_privilege(current_user,'app.get_order_updates(text,text,text)','EXECUTE'),has_function_privilege('warehouse_loader','app.get_order_updates(text,text,text)','EXECUTE'),has_table_privilege(current_user,'ref.package_types','INSERT')").fetchone()
    passed=all(r['missing_or_different_rows']==r['extra_or_different_rows']==0 for r in reports.values()) and privileges==(True,False,False)
    print(json.dumps({'verified_at':datetime.now(timezone.utc).isoformat(),'passed':passed,'procedure':manifest['source_procedure'],
        'source_definition_sha256':manifest['definition_sha256'],'reference_version':manifest['reference_version'],'columns':expected_columns,'cases':reports,
        'source_return_status':0,'target_success':'SELECT result set; no T-SQL return-code emulation',
        'ordering':'OrderID ascending; duplicate-preserving comparison within each OrderID because the source does not define a line tie-breaker',
        'reader_execute':privileges[0],'loader_execute':privileges[1],'reader_can_modify_reference':privileges[2],
        'limits':['Quiet fixture comparison; not an online cutover proof','Explicit reference snapshot; no automatic refresh',
            'datetime2(7) exposed as exact ISO text; cutoff inputs restricted to ISO without timezone','Source EXECUTE AS OWNER is compared with the full-scope lab reader; per-application RLS mapping is not certified',
            'Current-row extraction preserves the source procedure semantics; deletes and historical versions are not returned'],
        'purge_authorized':False},indent=2))
    raise SystemExit(0 if passed else 1)


if __name__=='__main__': main()
