"""Refresh protected SQL metadata and publish a definition-free migration catalog."""
import argparse
from collections import Counter
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

service=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('source_export',service/'scripts/source-export.py')
source=importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)
PILOT='Integration.GetOrderUpdates'
CAPTURED={'Sales.Customers','Sales.Orders','Sales.OrderLines','Sales.Invoices','Sales.InvoiceLines'}

METADATA="""
SELECT CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS nvarchar(128)) AS database_collation,
 JSON_QUERY((SELECT SCHEMA_NAME(o.schema_id)+'.'+o.name AS object_name,p.parameter_id,p.name,TYPE_NAME(p.user_type_id) AS data_type,
 p.max_length,p.precision,p.scale,p.is_output,p.is_readonly FROM sys.parameters p JOIN sys.objects o ON o.object_id=p.object_id
 WHERE o.is_ms_shipped=0 AND SCHEMA_NAME(o.schema_id)<>'cdc' ORDER BY object_name,p.parameter_id FOR JSON PATH,INCLUDE_NULL_VALUES)) AS parameters,
 JSON_QUERY((SELECT SCHEMA_NAME(o.schema_id)+'.'+o.name AS object_name,m.uses_ansi_nulls,m.uses_quoted_identifier,
 m.execute_as_principal_id,USER_NAME(m.execute_as_principal_id) AS execution_principal
 FROM sys.sql_modules m JOIN sys.objects o ON o.object_id=m.object_id WHERE o.is_ms_shipped=0 AND SCHEMA_NAME(o.schema_id)<>'cdc'
 FOR JSON PATH,INCLUDE_NULL_VALUES)) AS modules,
 JSON_QUERY((SELECT column_ordinal,name,system_type_name,is_nullable,collation_name,error_number,error_message
 FROM sys.dm_exec_describe_first_result_set_for_object(OBJECT_ID(N'Integration.GetOrderUpdates'),0)
 ORDER BY column_ordinal FOR JSON PATH,INCLUDE_NULL_VALUES)) AS pilot_result_columns,
 JSON_QUERY((SELECT SCHEMA_NAME(t.schema_id)+'.'+t.name AS table_name,c.name AS column_name,c.collation_name
 FROM sys.columns c JOIN sys.tables t ON t.object_id=c.object_id WHERE c.collation_name IS NOT NULL AND t.is_ms_shipped=0
 AND SCHEMA_NAME(t.schema_id)<>'cdc' FOR JSON PATH)) AS column_collations,
 JSON_QUERY((SELECT s.name AS schema_name,p.name,p.is_enabled,sp.predicate_definition,OBJECT_SCHEMA_NAME(sp.target_object_id)+'.'+OBJECT_NAME(sp.target_object_id) AS target_table
 FROM sys.security_policies p JOIN sys.schemas s ON s.schema_id=p.schema_id JOIN sys.security_predicates sp ON sp.object_id=p.object_id
 FOR JSON PATH)) AS security_policies,
 JSON_QUERY((SELECT OBJECT_SCHEMA_NAME(d.major_id)+'.'+OBJECT_NAME(d.major_id) AS object_name,USER_NAME(d.grantee_principal_id) AS grantee,
 d.permission_name,d.state_desc FROM sys.database_permissions d WHERE d.class=1 AND OBJECT_SCHEMA_NAME(d.major_id)<>'cdc'
 FOR JSON PATH)) AS object_permissions,
 JSON_QUERY((SELECT OBJECT_SCHEMA_NAME(object_id)+'.'+OBJECT_NAME(object_id) AS object_name,cached_time,last_execution_time,execution_count
 FROM sys.dm_exec_procedure_stats WHERE database_id=DB_ID() AND OBJECT_SCHEMA_NAME(object_id)<>'cdc' FOR JSON PATH)) AS cache_observations
FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;
"""


def classify(name):
    schema,short=name.split('.',1)
    if schema in {'Application','Sequences'}:
        return 'source_administration','Retain with the source; redesign target administration explicitly.'
    if schema=='DataLoadSimulation':
        return 'lab_simulation','Keep as optional lab tooling; do not migrate into warehouse query APIs.'
    if schema=='Integration':
        return 'data_extraction','Port the extraction contract after covering dependencies and cutoff semantics.'
    if schema=='Website' and short.startswith('SearchFor'):
        return 'application_read','Candidate for a read API after data, search, JSON and authorization parity review.'
    if schema=='Website':
        return 'application_write','Design a transactional application target; do not redirect writes into the Data Vault.'
    return 'manual_review','Determine behavior and ownership before choosing a target.'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True)
    args=parser.parse_args()
    os.umask(0o077)
    directory=Path(args.output_dir)
    directory.mkdir(parents=True,exist_ok=False,mode=0o700)
    inventory=source.query(source.INVENTORY)
    metadata=source.query(METADATA)
    captured_at=datetime.now(timezone.utc).isoformat()
    inventory['captured_at']=metadata['captured_at']=captured_at
    for name,value in [('inventory.json',inventory),('metadata.json',metadata)]:
        (directory/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    objects={o['schema_name']+'.'+o['name']:o for o in inventory['objects']}
    edges={name:[] for name in objects}
    for d in inventory['dependencies']:
        parent=d['referencing_schema']+'.'+d['referencing_object']
        edges.setdefault(parent,[]).append(d)
    def closure(name,seen=None):
        seen=set() if seen is None else seen
        if name in seen:
            return set(),set()
        seen.add(name)
        tables,unresolved=set(),set()
        for d in edges.get(name,[]):
            target=(d['referenced_schema_name'] or '?')+'.'+(d['referenced_entity_name'] or '?')
            if d['referenced_server_name'] or d['referenced_database_name'] or target not in objects:
                unresolved.add(target)
            elif objects[target]['type_desc']=='USER_TABLE':
                tables.add(target)
            else:
                nested,unknown=closure(target,seen)
                tables.update(nested); unresolved.update(unknown)
        return tables,unresolved
    records=[]
    for name,obj in sorted(objects.items()):
        if obj['type_desc']!='SQL_STORED_PROCEDURE':
            continue
        definition=obj['definition'] or ''
        category,action=classify(name)
        tables,unresolved=closure(name)
        dynamic=bool(re.search(r'\bEXEC(?:UTE)?\s*(?:\(|@|(?:\[?sys\]?\.)?\[?sp_executesql\b)',definition,re.I))
        records.append({'source_object':name,'definition_sha256':hashlib.sha256(definition.encode()).hexdigest(),
            'category':category,'proposed_action':action,'assessment_basis':'Source definitions and catalog dependency graph; application ownership is unconfirmed.',
            'parameters':[p for p in metadata['parameters'] if p['object_name']==name],
            'execution_context':next((m for m in metadata['modules'] if m['object_name']==name),None),
            'direct_references':sorted(set((d['referenced_schema_name'] or '?')+'.'+(d['referenced_entity_name'] or '?') for d in edges.get(name,[]))),
            'transitive_table_dependencies':sorted(tables),'outside_current_cdc_scope':sorted(tables-CAPTURED),
            'unresolved_or_external_references':sorted(unresolved),'dynamic_sql_review_required':dynamic,
            'referencing_sql_objects':sorted(n for n,ds in edges.items() if any((d['referenced_schema_name'] or '?')+'.'+(d['referenced_entity_name'] or '?')==name for d in ds)),
            'application_owner':None,'external_callers':'Not established; confirm with application owners and runtime observations.',
            'source_retirement_authorized':False,'migration_status':'selected_pilot' if name==PILOT else 'assessed_not_migrated'})
    reference=source.query("SELECT JSON_QUERY((SELECT PackageTypeID,PackageTypeName,ValidFrom,ValidTo FROM Warehouse.PackageTypes ORDER BY PackageTypeID FOR JSON PATH,INCLUDE_NULL_VALUES)) AS rows FOR JSON PATH,WITHOUT_ARRAY_WRAPPER")
    reference.update(source_table='Warehouse.PackageTypes',captured_at=captured_at,projection=['PackageTypeID','PackageTypeName','ValidFrom','ValidTo'])
    data=json.dumps(reference['rows'],ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
    reference['sha256']=hashlib.sha256(data).hexdigest()
    reference['version']='wwi-package-types/v1/'+reference['sha256']
    (directory/'package-types.json').write_text(json.dumps(reference,ensure_ascii=False,indent=2)+'\n')
    report={'captured_at':captured_at,'source_inventory_path':str(directory/'inventory.json'),
        'source_inventory_sha256':hashlib.sha256((directory/'inventory.json').read_bytes()).hexdigest(),
        'procedure_count':len(records),'classification_counts':dict(Counter(r['category'] for r in records)),
        'procedures':records,'views_and_functions':[{'name':n,'type':o['type_desc'],'definition_sha256':hashlib.sha256((o['definition'] or '').encode()).hexdigest(),
            'transitive_tables':sorted(closure(n)[0])} for n,o in sorted(objects.items()) if o['type_desc'] in {'VIEW','SQL_SCALAR_FUNCTION','SQL_INLINE_TABLE_VALUED_FUNCTION','SQL_TABLE_VALUED_FUNCTION'}],
        'dml_triggers':[n for n,o in objects.items() if o['type_desc']=='SQL_TRIGGER'],
        'sequence_count':sum(o['type_desc']=='SEQUENCE_OBJECT' for o in objects.values()),
        'security_policies':metadata['security_policies'],
        'sql_agent_jobs':[{'name':j['name'],'enabled':bool(j['enabled']),'step_id':j['step_id'],'subsystem':j['subsystem'],'database':j['database_name'],
            'command_sha256':hashlib.sha256((j['command'] or '').encode()).hexdigest(),
            'exact_name_mentions':[r['source_object'] for r in records if r['source_object'].lower() in (j['command'] or '').replace('[','').replace(']','').lower()]} for j in inventory['agent_job_steps']],
        'cache_observation_count':len(metadata['cache_observations'] or []),
        'limits':['Static metadata is not a complete call graph; dynamic SQL, unqualified references, CLR methods, external clients and runtime permissions need review.',
            'No cache observation does not prove a procedure is unused. No procedure is classified as safe to delete.',
            'Schema/name-based application groupings are not confirmed application ownership.'],
        'selected_pilot':PILOT,'reference_snapshot':{'version':reference['version'],'rows':len(reference['rows']),'automatic_refresh':False},
        'purge_authorized':False}
    (directory/'catalog.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'directory':str(directory),'procedures':len(records),'categories':report['classification_counts'],
        'dynamic_sql_candidates':sum(r['dynamic_sql_review_required'] for r in records),'reference_rows':len(reference['rows']),
        'pilot_columns':metadata['pilot_result_columns']}))


if __name__=='__main__':
    main()
