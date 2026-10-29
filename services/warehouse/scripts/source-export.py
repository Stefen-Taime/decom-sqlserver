"""VM-only read-only SQL Server inventory and pilot reconciliation projection."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess


def query(sql):
    # Unlike the interactive wrapper, unlimited variable-width output is needed here.
    command = 'export SQLCMDPASSWORD="$(cat /run/secrets/sa_password)"; exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -l 10 -y 0 -w 65535'
    result = subprocess.run(['docker', 'exec', '-i', 'decom-sqlserver-sqlserver-1',
        '/bin/bash', '-c', command], input='SET NOCOUNT ON;\nUSE WideWorldImporters;\n' + sql,
        text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    # FOR JSON may be split into multiple output chunks; concatenate them losslessly.
    text = ''.join(result.stdout.splitlines())
    return json.loads(text[text.index('{'):text.rindex('}')+1])


INVENTORY = """
SELECT
 JSON_QUERY((SELECT DB_NAME() AS name, snapshot_isolation_state_desc FROM sys.databases WHERE name=DB_NAME() FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)) AS database_info,
 JSON_QUERY((SELECT o.object_id, SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type_desc,
   o.create_date, o.modify_date, m.definition,
   CASE WHEN m.definition LIKE '%sp_executesql%' OR m.definition LIKE '%EXEC(%' OR m.definition LIKE '%EXEC (%' THEN 1 ELSE 0 END AS dynamic_sql_review
   FROM sys.objects o LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id
   WHERE o.is_ms_shipped=0 AND SCHEMA_NAME(o.schema_id) NOT IN ('cdc','sys')
   ORDER BY schema_name,o.name FOR JSON PATH, INCLUDE_NULL_VALUES)) AS objects,
 JSON_QUERY((SELECT SCHEMA_NAME(t.schema_id) AS schema_name,t.name AS table_name,c.column_id,c.name,
   TYPE_NAME(c.user_type_id) AS data_type,c.max_length,c.precision,c.scale,c.is_nullable,c.is_identity,c.is_computed,
   dc.definition AS default_expression FROM sys.tables t JOIN sys.columns c ON c.object_id=t.object_id
   LEFT JOIN sys.default_constraints dc ON dc.object_id=c.default_object_id
   WHERE t.is_ms_shipped=0 AND SCHEMA_NAME(t.schema_id)<>'cdc' ORDER BY schema_name,table_name,c.column_id FOR JSON PATH,INCLUDE_NULL_VALUES)) AS columns,
 JSON_QUERY((SELECT OBJECT_SCHEMA_NAME(d.referencing_id) AS referencing_schema,OBJECT_NAME(d.referencing_id) AS referencing_object,
   d.referenced_server_name,d.referenced_database_name,d.referenced_schema_name,d.referenced_entity_name,d.is_ambiguous
   FROM sys.sql_expression_dependencies d WHERE OBJECT_SCHEMA_NAME(d.referencing_id)<>'cdc'
   FOR JSON PATH,INCLUDE_NULL_VALUES)) AS dependencies,
 JSON_QUERY((SELECT fk.name,OBJECT_SCHEMA_NAME(fk.parent_object_id) AS parent_schema,OBJECT_NAME(fk.parent_object_id) AS parent_table,
   COL_NAME(fkc.parent_object_id,fkc.parent_column_id) AS parent_column,
   OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
   COL_NAME(fkc.referenced_object_id,fkc.referenced_column_id) AS referenced_column
   FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
   FOR JSON PATH)) AS foreign_keys,
 JSON_QUERY((SELECT name,base_object_name FROM sys.synonyms FOR JSON PATH)) AS synonyms,
 JSON_QUERY((SELECT j.name,j.enabled,s.step_id,s.step_name,s.subsystem,s.database_name,s.command
   FROM msdb.dbo.sysjobs j LEFT JOIN msdb.dbo.sysjobsteps s ON j.job_id=s.job_id
   ORDER BY j.name,s.step_id FOR JSON PATH,INCLUDE_NULL_VALUES)) AS agent_job_steps
FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;
"""

# Explicit projection: exact identifiers, business attributes and dates, not row counts alone.
# Spatial DeliveryLocation and SQL temporal/audit metadata are retained in raw archives but
# are not covered by this first cross-engine business reconciliation.
CUSTOMER_COLUMNS = 'CustomerID,CustomerName,BillToCustomerID,CustomerCategoryID,BuyingGroupID,PrimaryContactPersonID,AlternateContactPersonID,DeliveryMethodID,DeliveryCityID,PostalCityID,CreditLimit,AccountOpenedDate,StandardDiscountPercentage,IsStatementSent,IsOnCreditHold,PaymentDays,PhoneNumber,FaxNumber,DeliveryRun,RunPosition,WebsiteURL,DeliveryAddressLine1,DeliveryAddressLine2,DeliveryPostalCode,PostalAddressLine1,PostalAddressLine2,PostalPostalCode'
ORDER_COLUMNS = 'OrderID,CustomerID,SalespersonPersonID,PickedByPersonID,ContactPersonID,BackorderOrderID,OrderDate,ExpectedDeliveryDate,CustomerPurchaseOrderNumber,IsUndersupplyBackordered,Comments,DeliveryInstructions,InternalComments,PickingCompletedWhen'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=['inventory', 'reconcile'])
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.kind == 'inventory':
        result = query(INVENTORY)
        result['review_status'] = 'Metadata baseline only; application owners, dynamic SQL, external callers and runtime usage require review.'
        summary = dict(Counter(o['type_desc'] for o in result['objects']))
    else:
        result = query(f"""
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION;
SELECT JSON_QUERY((SELECT {CUSTOMER_COLUMNS} FROM Sales.Customers ORDER BY CustomerID FOR JSON PATH,INCLUDE_NULL_VALUES)) AS customers,
       JSON_QUERY((SELECT {ORDER_COLUMNS} FROM Sales.Orders ORDER BY OrderID FOR JSON PATH,INCLUDE_NULL_VALUES)) AS orders
FOR JSON PATH,WITHOUT_ARRAY_WRAPPER;
COMMIT;
""")
        result['projection'] = {'Sales.Customers': CUSTOMER_COLUMNS.split(','), 'Sales.Orders': ORDER_COLUMNS.split(',')}
        summary = {key: len(result[key]) for key in ['customers', 'orders']}
    result['captured_at'] = datetime.now(timezone.utc).isoformat()
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    path.chmod(0o600)
    print(json.dumps({'kind': args.kind, 'output': str(path), 'summary': summary}))


if __name__ == '__main__':
    main()
