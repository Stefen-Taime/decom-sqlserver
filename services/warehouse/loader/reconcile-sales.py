"""Stream full ordered primary-key sets and explicit business projections; no float money."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from psycopg.types.json import set_json_loads
from load import canonical,connect
from reconcile import normalize as pilot_normalize
from sales_contract import TABLE_SPECS


def normalize(value,column):
    if value is None:
        return None
    if column in {'UnitPrice','TaxRate','TaxAmount','LineProfit','ExtendedPrice'}:
        return format(Decimal(str(value)).normalize(),'f')
    if column=='InvoiceDate':
        return value.removesuffix('Z')
    if column=='ConfirmedDeliveryTime':
        value=value.removesuffix('Z')
        return value.rstrip('0').rstrip('.') if '.' in value else value
    return pilot_normalize(value,column)


def check(directory,conn):
    manifest=json.loads((directory/'manifest.json').read_text())
    if set(manifest['tables'])!=set(TABLE_SPECS):
        raise ValueError('Incomplete source baseline')
    set_json_loads(lambda value:json.loads(value,parse_float=Decimal),conn)
    tables={}
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        for table,spec in TABLE_SPECS.items():
            metadata=manifest['tables'][table]
            columns=spec['columns'].split(',')
            if metadata['columns']!=columns or metadata['file']!=spec['view']+'.jsonl':
                raise ValueError('Unexpected source projection')
            source_hash,target_hash,file_hash=hashlib.sha256(),hashlib.sha256(),hashlib.sha256()
            def source_rows():
                previous=-1
                with (directory/metadata['file']).open('rb') as source:
                    for line in source:
                        file_hash.update(line)
                        row=json.loads(line,parse_float=Decimal)
                        pk=row[spec['pk']]
                        if pk<=previous or set(row)!=set(columns):
                            raise ValueError('Source baseline keys or columns changed')
                        previous=pk
                        yield pk,[normalize(row[col],col) for col in columns]
            with conn.cursor(name='sales_reconcile') as cursor:
                extra=',confirmed_delivery_time,confirmed_received_by' if table=='Sales.Invoices' else ''
                cursor.itersize=1000
                cursor.execute(f"SELECT {spec['id_column']},attributes{extra} FROM mart.{spec['view']} ORDER BY {spec['id_column']}")
                def target_rows():
                    for item in cursor:
                        pk,attributes=item[:2]
                        row={**attributes,spec['pk']:pk}
                        if table=='Sales.Invoices':
                            row.update(ConfirmedDeliveryTime=item[2],ConfirmedReceivedBy=item[3])
                        if any(col not in row for col in columns):
                            raise ValueError('Missing warehouse projection column: '+table)
                        yield pk,[normalize(row[col],col) for col in columns]
                left,right=iter(source_rows()),iter(target_rows())
                a,b=next(left,None),next(right,None)
                missing=extra_keys=mismatched=source_count=target_count=0
                differing=set()
                while a is not None or b is not None:
                    consume_a=b is None or (a is not None and a[0]<=b[0])
                    consume_b=a is None or (b is not None and b[0]<=a[0])
                    if consume_a and consume_b and a[1]!=b[1]:
                        mismatched+=1
                        differing.update(col for col,x,y in zip(columns,a[1],b[1]) if x!=y)
                    missing+=int(consume_a and not consume_b)
                    extra_keys+=int(consume_b and not consume_a)
                    if consume_a:
                        source_hash.update((canonical(a)+'\n').encode())
                        source_count+=1
                        a=next(left,None)
                    if consume_b:
                        target_hash.update((canonical(b)+'\n').encode())
                        target_count+=1
                        b=next(right,None)
            if file_hash.hexdigest()!=metadata['file_sha256'] or source_count!=metadata['rows']:
                raise ValueError('Source baseline file checksum/count mismatch')
            tables[table]={'source_rows':source_count,'warehouse_rows':target_count,'checked_columns':columns,
                'missing_keys':missing,'extra_keys':extra_keys,'mismatched_rows':mismatched,'differing_columns':sorted(differing),
                'source_projection_sha256':source_hash.hexdigest(),'warehouse_projection_sha256':target_hash.hexdigest()}
        relationships={}
        for name,query in {
            'order_customers':'SELECT count(*) FROM mart.orders x LEFT JOIN mart.customers p USING(customer_id) WHERE p.customer_id IS NULL',
            'order_line_orders':'SELECT count(*) FROM mart.order_lines x LEFT JOIN mart.orders p USING(order_id) WHERE p.order_id IS NULL',
            'invoice_customers':'SELECT count(*) FROM mart.invoices x LEFT JOIN mart.customers p USING(customer_id) WHERE p.customer_id IS NULL',
            'invoice_bill_to_customers':'SELECT count(*) FROM mart.invoices x LEFT JOIN mart.customers p ON p.customer_id=x.bill_to_customer_id WHERE p.customer_id IS NULL',
            'invoice_orders':'SELECT count(*) FROM mart.invoices x LEFT JOIN mart.orders p USING(order_id) WHERE x.order_id IS NOT NULL AND p.order_id IS NULL',
            'invoice_line_invoices':'SELECT count(*) FROM mart.invoice_lines x LEFT JOIN mart.invoices p USING(invoice_id) WHERE p.invoice_id IS NULL'
        }.items():
            relationships[name]=conn.execute(query).fetchone()[0]
        grains={}
        for view,key,base in [('order_details','order_line_id','order_lines'),('invoice_details','invoice_line_id','invoice_lines'),('invoice_summary','invoice_id','invoices')]:
            count,distinct=conn.execute(f'SELECT count(*),count(DISTINCT {key}) FROM mart.{view}').fetchone()
            expected=conn.execute(f'SELECT count(*) FROM mart.{base}').fetchone()[0]
            grains[view]={'rows':count,'distinct_keys':distinct,'expected_rows':expected}
        totals={}
        for column in ['quantity','tax_amount','line_profit','extended_price']:
            values=[conn.execute(f'SELECT sum({column}) FROM mart.{view}').fetchone()[0] for view in ['invoice_lines','invoice_details','invoice_summary']]
            totals[column]={'invoice_lines':str(values[0]),'invoice_details':str(values[1]),'invoice_summary':str(values[2])}
            if not values[0]==values[1]==values[2]:
                raise ValueError('Invoice aggregates changed across joins: '+column)
    passed=all(not t['missing_keys'] and not t['extra_keys'] and not t['mismatched_rows'] for t in tables.values()) and not any(relationships.values()) and all(g['rows']==g['distinct_keys']==g['expected_rows'] for g in grains.values())
    return {'passed':passed,'tables':tables,'orphan_relationships':relationships,'view_grains':grains,'invoice_totals':totals,
        'source_export_at':manifest['captured_at'],'scope':'Full business primary-key sets and explicit projections, excluding spatial and audit columns; quiet fixture validation, not an online cutover checkpoint.', 'purge_authorized':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source_directory')
    args=parser.parse_args()
    with connect() as conn:
        result=check(Path(args.source_directory),conn)
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['passed'] else 1)
