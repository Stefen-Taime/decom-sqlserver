"""Fixed, bounded CDC queries through a dedicated SQL Server read-only account."""
import json
from pathlib import Path
import re
import subprocess

ROOT = Path('/srv/decom-sqlserver')
SERVICE = ROOT / 'workspace/services/purge'


def column_expression(column, alias='ct'):
    name=column['name'];kind=column['type']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',name) or alias not in {'ct','s'}:
        raise ValueError('Unsupported captured column identifier')
    field=f'{alias}.[{name}]'
    if kind in {'char','nchar','varchar','nvarchar','text','ntext','bit'}:
        expression=field
    elif kind in {'money','smallmoney'}:
        expression=f'CONVERT(nvarchar(128),{field},2)'
    elif kind in {'tinyint','smallint','int','bigint','decimal','numeric','uniqueidentifier'}:
        expression=f'CONVERT(nvarchar(128),{field})'
    elif kind in {'date','time','datetime','datetime2','smalldatetime'}:
        expression=f'CONVERT(nvarchar(50),{field},126)'
    elif kind in {'geography','geometry'}:
        expression=f'{field}.Serialize()'
    elif kind in {'binary','varbinary','image'}:
        expression=f'CONVERT(varbinary(max),{field})'
    else:
        raise ValueError('Unsupported captured SQL type: '+kind)
    return expression+f' AS [{name}]'


def captured_columns(policy):
    value=query("""SELECT JSON_QUERY((SELECT ct.capture_instance,cc.column_name AS name,
        cc.column_ordinal AS ordinal,TYPE_NAME(c.user_type_id) AS type,c.max_length,c.precision,c.scale,c.is_nullable,
        COLUMNPROPERTY(ct.source_object_id,cc.column_name,'IsComputed') AS source_is_computed
        FROM cdc.change_tables ct JOIN cdc.captured_columns cc ON cc.object_id=ct.object_id
        JOIN sys.columns c ON c.object_id=ct.object_id AND c.name=cc.column_name
        ORDER BY ct.capture_instance,cc.column_ordinal FOR JSON PATH,INCLUDE_NULL_VALUES)) AS columns
        FOR JSON PATH,WITHOUT_ARRAY_WRAPPER;""")
    result={table:[{k:v for k,v in column.items() if k!='capture_instance'} for column in value['columns']
                   if column['capture_instance']==spec['capture_instance']] for table,spec in policy['tables'].items()}
    for table,columns in result.items():
        if not columns or policy['tables'][table]['primary_key'] not in {c['name'] for c in columns}:
            raise ValueError('Missing captured column metadata: '+table)
        for column in columns: column_expression(column)
    return result


def query(sql, password=None):
    password = password or (ROOT / 'secrets/purge/sql_password').read_text().strip()
    shell = ('IFS= read -r SQLCMDPASSWORD; export SQLCMDPASSWORD; '
             'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U cdc_auditor -C -b -l 10 -t 60 -y 0 -w 65535')
    result = subprocess.run(['docker', 'exec', '-i', 'decom-sqlserver-sqlserver-1', '/bin/bash', '-c', shell],
        input=password + '\nSET NOCOUNT ON; SET LOCK_TIMEOUT 5000; USE WideWorldImporters;\n' + sql,
        text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError('Read-only SQL Server audit query failed: ' + (result.stdout + result.stderr).replace(password, '[REDACTED]'))
    raw = ''.join(result.stdout.splitlines())
    return json.loads(raw[raw.index('{'):raw.rindex('}')+1])


def collect(policy, upper=None, columns=None):
    if upper is not None and not re.fullmatch(r'0x[0-9a-fA-F]{20}', upper):
        raise ValueError('Invalid fixed observation boundary')
    columns=columns if columns is not None else captured_columns(policy)
    parts = []
    for table, spec in policy['tables'].items():
        capture, pk = spec['capture_instance'], spec['primary_key']
        if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', v) for v in [capture, pk]):
            raise ValueError('Invalid operator policy identifier')
        limit = policy['max_source_rows_per_table']
        if type(limit) is not int or not 1 <= limit <= 100000:
            raise ValueError('Invalid audit scan limit')
        # sqlcmd's input handling can split long physical lines inside an identifier.
        # Keep generated statements multiline even when the capture has many columns.
        projection=',\n'.join(column_expression(column) for column in columns[table])
        parts.append(f"JSON_QUERY((SELECT TOP({limit+1}) CONVERT(varchar(22),__$start_lsn,1) AS commit_lsn,"
            f"CONVERT(varchar(22),__$seqval,1) AS change_lsn,__$operation AS operation,__$command_id AS command_id,"
            "CONVERT(varchar(max),__$update_mask,1) AS update_mask,"
            f"CONVERT(nvarchar(100),[{pk}]) AS pk,"
            f"JSON_QUERY((SELECT {projection} FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES)) AS row_data "
            f"FROM cdc.[{capture}_CT] ct WHERE __$start_lsn<=@upper "
            "ORDER BY __$start_lsn,__$seqval,__$command_id,__$operation FOR JSON PATH)) AS [" + capture + "]")
    boundary = upper or 'sys.fn_cdc_get_max_lsn()'
    hours = policy['retention_hours']
    if type(hours) is not int or not 24 <= hours <= 8760:
        raise ValueError('Invalid retention margin')
    sql = f"DECLARE @upper binary(10)={boundary}; DECLARE @cutoff datetime=DATEADD(hour,-{hours},GETDATE());\nSELECT "
    sql += "CONVERT(varchar(22),@upper,1) AS upper_lsn,CONVERT(varchar(40),SYSDATETIMEOFFSET(),127) AS captured_at,"
    sql += "CONVERT(varchar(40),@cutoff,126) AS retention_cutoff_source_time,"
    sql += "CONVERT(varchar(22),sys.fn_cdc_map_time_to_lsn('largest less than or equal',@cutoff),1) AS retention_observation_lsn,"
    sql += "JSON_QUERY((SELECT capture_instance,OBJECT_SCHEMA_NAME(source_object_id)+'.'+OBJECT_NAME(source_object_id) AS table_name,"
    sql += "CONVERT(varchar(22),start_lsn,1) AS start_lsn,CONVERT(varchar(22),end_lsn,1) AS end_lsn,has_drop_pending,role_name "
    sql += "FROM cdc.change_tables ORDER BY capture_instance FOR JSON PATH,INCLUDE_NULL_VALUES)) AS capture_instances,"
    sql += 'JSON_QUERY((SELECT ' + ',\n'.join(parts) + ' FOR JSON PATH,WITHOUT_ARRAY_WRAPPER)) AS tables '
    sql += 'FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;'
    result = query(sql)
    result['observation_format']='sqlserver-cdc-row-values/v1'
    result['captured_columns']=columns
    raw_tables=result.get('tables') or {}
    if set(raw_tables)-{spec['capture_instance'] for spec in policy['tables'].values()}:
        raise ValueError('Unexpected source JSON structure; dotted table names must not become nested objects')
    result['tables'] = {table: raw_tables.get(spec['capture_instance']) or [] for table,spec in policy['tables'].items()}
    if any(len(rows) > policy['max_source_rows_per_table'] for rows in result['tables'].values()):
        raise RuntimeError('Retained CDC exceeds the bounded audit limit; partition the audit before continuing')
    return result
