"""Read-only reconciliation of current pilot rows against an exported SQL Server projection."""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import re
from load import canonical, connect, digest


def normalize(value, column):
    if value is None:
        return None
    if column in {'CreditLimit','StandardDiscountPercentage'}:
        return format(Decimal(str(value)).normalize(),'f')
    if column in {'AccountOpenedDate','OrderDate','ExpectedDeliveryDate'}:
        # Debezium IsoDate serializes SQL DATE as YYYY-MM-DDZ; the source has no timezone.
        value=value.removesuffix('Z')
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
            raise ValueError('Unexpected date representation: '+column)
    if column=='PickingCompletedWhen':
        value=value.removesuffix('Z')
        if '.' in value:
            value=value.rstrip('0').rstrip('.')
    return value


def check(source,conn):
    result={}
    for table,field,view,key in [('Sales.Customers','customers','customers','CustomerID'),('Sales.Orders','orders','orders','OrderID')]:
        columns=source['projection'][table]
        source_rows={row[key]:[normalize(row.get(col),col) for col in columns] for row in source[field]}
        id_column='customer_id' if field=='customers' else 'order_id'
        target_rows={}
        for pk,attributes in conn.execute(f'SELECT {id_column},attributes FROM mart.{view}'):
            row={**attributes,key:pk}
            if not all(col in row for col in columns):
                raise ValueError('A projected source column is missing from the warehouse')
            target_rows[pk]=[normalize(row[col],col) for col in columns]
        missing=set(source_rows)-set(target_rows)
        extra=set(target_rows)-set(source_rows)
        mismatched=[pk for pk in source_rows.keys() & target_rows.keys() if source_rows[pk]!=target_rows[pk]]
        result[table]={'source_rows':len(source_rows),'warehouse_rows':len(target_rows),'checked_columns':columns,
            'missing_keys':len(missing),'extra_keys':len(extra),'mismatched_rows':len(mismatched),
            'source_projection_sha256':digest(sorted(source_rows.items())), 'warehouse_projection_sha256':digest(sorted(target_rows.items()))}
        if missing or extra or mismatched:
            differences={col: {'source_type':type(source_rows[pk][i]).__name__,'warehouse_type':type(target_rows[pk][i]).__name__}
                for pk in mismatched[:1] for i,col in enumerate(columns) if source_rows[pk][i]!=target_rows[pk][i]}
            raise ValueError(canonical({'table':table,'result':result[table],'differing_columns_example':differences}))
    orphans=conn.execute('SELECT count(*) FROM mart.orders o LEFT JOIN mart.customers c USING(customer_id) WHERE c.customer_id IS NULL').fetchone()[0]
    if orphans:
        raise ValueError('Current order/customer relation has missing customers')
    return {'tables':result,'orphan_order_customers':orphans,'source_export_at':source['captured_at'],
        'scope':'Business projection and full primary-key sets. Excludes spatial data and temporal/audit metadata; not a general online cutover checkpoint.',
        'purge_authorized':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source_export')
    args=parser.parse_args()
    source=json.loads(Path(args.source_export).read_text(),parse_float=Decimal)
    with connect() as connection:
        print(json.dumps(check(source,connection),indent=2))
