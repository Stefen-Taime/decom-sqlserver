"""Fixed-scope lab.CdcProbe cleanup rules. Never a business-table purge approval."""
from collections import Counter,defaultdict
import hashlib
import json
import re
from audit_core import lsn,image_key

TABLE='lab.CdcProbe'
CAPTURE='lab_CdcProbe'
RETENTION_MINUTES=15
MAX_ROWS=200


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def row_key(row):
    return image_key(TABLE,row['pk'],row['operation'],row['commit_lsn'],row['change_lsn'])


def row_signature(row):
    return (row_key(row),row['command_id'],row['update_mask'].lower(),
            str(row['row_data']['ProbeID']).lower(),row['row_data']['Payload'])


def plan(source,metadata,checkpoint,anchor_id):
    capture=metadata['capture'];point=checkpoint['checkpoint']
    if capture['capture_instance']!=CAPTURE or capture['table_name']!=TABLE:
        raise ValueError('Pilot scope must be exactly lab.CdcProbe')
    if metadata['live_probe_rows']!=0: raise ValueError('Technical table still has live probe rows')
    jobs=metadata['cleanup_jobs']
    if len(jobs)!=1 or jobs[0]['enabled']!=0: raise ValueError('Automatic cleanup must stay disabled')
    boundary=lsn(metadata['effective_lsn'])
    if not lsn(capture['start_lsn'])<boundary<lsn(point['commit_lsn']):
        raise ValueError('Boundary must advance and remain strictly before the persisted checkpoint transaction')
    if point['partition']!={'server':'wwi','database':'WideWorldImporters'}:
        raise ValueError('Checkpoint belongs to another source')
    rows=source['tables'][TABLE]
    if len(rows)>MAX_ROWS+20: raise ValueError('Technical history exceeds pilot bounds')
    selected=[r for r in rows if lsn(r['commit_lsn'])<boundary]
    if not 0<len(selected)<=MAX_ROWS or len(selected)!=metadata['candidate_count']:
        raise ValueError('Candidate count empty, changed or above the hard limit')
    if Counter(map(row_signature,selected))!=Counter(map(row_signature,metadata['candidates'])):
        raise ValueError('Independent candidate observations differ')
    if metadata['youngest_candidate_age_seconds']<RETENTION_MINUTES*60:
        raise ValueError('A candidate is newer than the pilot retention margin')
    groups=defaultdict(list)
    for row in rows: groups[str(row['pk']).lower()].append(row)
    ids={str(r['pk']).lower() for r in selected}
    if anchor_id.lower() in ids: raise ValueError('The new anchor must remain in CDC')
    for identity in ids|{anchor_id.lower()}:
        group=groups[identity]
        if Counter(r['operation'] for r in group)!=Counter([1,2,3,4]):
            raise ValueError('Only complete isolated insert/update/delete cycles are allowed')
        if identity in ids and any(lsn(r['commit_lsn'])>=boundary for r in group):
            raise ValueError('Boundary splits a technical probe lifecycle')
        for row in group:
            if set(row['row_data'])!={'ProbeID','Payload'} or str(row['row_data']['ProbeID']).lower()!=identity:
                raise ValueError('Unexpected probe schema or key')
            if row['row_data']['Payload']!=('before' if row['operation'] in {2,3} else 'after'):
                raise ValueError('Candidate is not the known technical fixture')
    return {'contract':'cdc-probe-purge-plan/v1','capture_instance':CAPTURE,'table':TABLE,
        'effective_lsn':boundary,'capture_before':capture,'candidate_count':len(selected),
        'candidate_cycles':len(ids),'candidate_sql_sha256':metadata['candidate_sql_sha256'].lower(),
        'candidate_rows_sha256':digest(sorted(map(row_signature,selected))),
        'retained_rows':len(rows)-len(selected),'retention_minutes':RETENTION_MINUTES,
        'max_rows':MAX_ROWS,'anchor_id':anchor_id,'checkpoint_commit_lsn':point['commit_lsn'],
        'selected_source_identities':[list(row_key(r)) for r in selected],
        'scope':'Manual POC only; lab.CdcProbe CDC history; no business cleanup approval'}


def verify_after(before,after,approved):
    """Reconcile the same fixed source interval, including untouched captures."""
    if before['upper_lsn']!=after['upper_lsn'] or before['captured_columns']!=after['captured_columns']:
        raise ValueError('Observation boundary or captured schema changed')
    if set(before['tables'])!=set(after['tables']): raise ValueError('Capture set changed')
    boundary=lsn(approved['effective_lsn']);removed=0
    for table,rows in before['tables'].items():
        expected=[r for r in rows if table!=TABLE or lsn(r['commit_lsn'])>=boundary]
        if Counter(map(digest,expected))!=Counter(map(digest,after['tables'][table])):
            raise ValueError('Unexpected source deletion or mutation: '+table)
        removed+=len(rows)-len(expected)
    if removed!=approved['candidate_count']: raise ValueError('Deletion count differs from the plan')
    expected_captures=[]
    for capture in before['capture_instances']:
        expected_captures.append({**capture,'start_lsn':'0x'+boundary} if capture['capture_instance']==CAPTURE else capture)
    def capture_digest(capture):
        return digest({**capture,'start_lsn':lsn(capture['start_lsn'])})
    if Counter(map(capture_digest,expected_captures))!=Counter(map(capture_digest,after['capture_instances'])):
        raise ValueError('Unexpected capture metadata change')
    return {'exact_planned_images_removed':removed,'retained_probe_images':len(after['tables'][TABLE]),
            'business_captures_unchanged':sorted(set(before['tables'])-{TABLE}),
            'fixed_upper_lsn':before['upper_lsn'],'all_retained_values_identical':True}


PROJECTION="""CONVERT(varchar(22),ct.__$start_lsn,1) AS commit_lsn,
CONVERT(varchar(22),ct.__$seqval,1) AS change_lsn,ct.__$operation AS operation,
ct.__$command_id AS command_id,CONVERT(varchar(max),ct.__$update_mask,1) AS update_mask,
CONVERT(nvarchar(100),ct.ProbeID) AS pk,
JSON_QUERY((SELECT CONVERT(nvarchar(100),ct.ProbeID) AS ProbeID,ct.Payload
 FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES)) AS row_data"""


def observation_sql(boundary=None):
    if boundary is not None: boundary='0x'+lsn(boundary)
    requested=boundary or "sys.fn_cdc_map_time_to_lsn('largest less than or equal',DATEADD(minute,-15,GETDATE()))"
    return f"""SET LOCK_TIMEOUT 5000;
DECLARE @requested binary(10)={requested};
DECLARE @effective binary(10)=(SELECT MIN(start_lsn) FROM cdc.lsn_time_mapping
 WHERE tran_end_time=(SELECT tran_end_time FROM cdc.lsn_time_mapping WHERE start_lsn=@requested));
DECLARE @rows nvarchar(max)=(SELECT TOP(201) {PROJECTION}
 FROM cdc.lab_CdcProbe_CT ct WHERE ct.__$start_lsn<@effective
 ORDER BY ct.__$start_lsn,ct.__$seqval,ct.__$command_id,ct.__$operation,ct.ProbeID FOR JSON PATH);
"""


METADATA_SQL="""
SELECT CONVERT(varchar(22),@requested,1) AS requested_lsn,
 CONVERT(varchar(22),@effective,1) AS effective_lsn,
 CONVERT(varchar(40),SYSDATETIMEOFFSET(),127) AS observed_at,
 JSON_QUERY((SELECT capture_instance,object_id,source_object_id,
 OBJECT_SCHEMA_NAME(source_object_id)+'.'+OBJECT_NAME(source_object_id) AS table_name,
 CONVERT(varchar(22),start_lsn,1) AS start_lsn,CONVERT(varchar(40),create_date,126) AS create_date
 FROM cdc.change_tables WHERE capture_instance='lab_CdcProbe'
 FOR JSON PATH,WITHOUT_ARRAY_WRAPPER)) AS capture,
 (SELECT COUNT_BIG(*) FROM lab.CdcProbe) AS live_probe_rows,
 (SELECT COUNT_BIG(*) FROM cdc.lab_CdcProbe_CT WHERE __$start_lsn<@effective) AS candidate_count,
 (SELECT MIN(DATEDIFF_BIG(second,m.tran_end_time,GETDATE())) FROM cdc.lab_CdcProbe_CT ct
 JOIN cdc.lsn_time_mapping m ON m.start_lsn=ct.__$start_lsn WHERE ct.__$start_lsn<@effective) AS youngest_candidate_age_seconds,
 LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@rows),2)) AS candidate_sql_sha256,
 JSON_QUERY(@rows) AS candidates,
 JSON_QUERY((SELECT j.enabled,c.retention FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id
 WHERE c.database_id=DB_ID() AND c.job_type='cleanup' FOR JSON PATH)) AS cleanup_jobs
FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;
"""


def execution_sql(approved):
    if approved['table']!=TABLE or approved['capture_instance']!=CAPTURE:
        raise ValueError('Business-table cleanup is not implemented')
    count=approved['candidate_count'];checksum=approved['candidate_sql_sha256']
    if type(count) is not int or not 0<count<=MAX_ROWS or not re.fullmatch('[0-9a-f]{64}',checksum):
        raise ValueError('Invalid bounded plan')
    capture=approved['capture_before']
    for field in ['object_id','source_object_id']:
        if type(capture[field]) is not int: raise ValueError('Invalid capture identity')
    boundary=lsn(approved['effective_lsn']);prior=lsn(capture['start_lsn'])
    checkpoint=lsn(approved['checkpoint_commit_lsn'])
    if not prior<boundary<checkpoint: raise ValueError('Unsafe cleanup boundary')
    return observation_sql(boundary)+f"""
IF DB_NAME()<>N'WideWorldImporters' THROW 51020,'Unexpected database',1;
IF @effective IS NULL OR @effective<>0x{boundary} THROW 51021,'Cleanup boundary changed',1;
IF NOT EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance=N'lab_CdcProbe'
 AND object_id={capture['object_id']} AND source_object_id={capture['source_object_id']}
 AND source_object_id=OBJECT_ID(N'lab.CdcProbe') AND object_id=OBJECT_ID(N'cdc.lab_CdcProbe_CT')
 AND start_lsn=0x{prior}) THROW 51022,'Capture identity changed',1;
IF (SELECT COUNT_BIG(*) FROM cdc.lab_CdcProbe_CT WHERE __$start_lsn<@effective)<>{count}
 OR HASHBYTES('SHA2_256',@rows)<>0x{checksum} THROW 51023,'Candidate set changed',1;
IF EXISTS(SELECT 1 FROM cdc.lab_CdcProbe_CT ct LEFT JOIN cdc.lsn_time_mapping m ON m.start_lsn=ct.__$start_lsn
 WHERE ct.__$start_lsn<@effective AND (m.tran_end_time IS NULL OR m.tran_end_time>DATEADD(minute,-15,GETDATE())))
 THROW 51024,'Retention margin violated',1;
IF EXISTS(SELECT 1 FROM lab.CdcProbe) THROW 51025,'Probe table is active',1;
IF (SELECT COUNT(*) FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j ON j.job_id=c.job_id
 WHERE c.database_id=DB_ID() AND c.job_type='cleanup' AND j.enabled=0)<>1
 THROW 51026,'Automatic cleanup must be disabled',1;
DECLARE @rc int,@failed bit=0;
EXEC @rc=sys.sp_cdc_cleanup_change_table @capture_instance=N'lab_CdcProbe',
 @low_water_mark=@effective,@threshold=100,@fCleanupFailed=@failed OUTPUT;
SELECT @rc AS return_code,@failed AS cleanup_failed,
 CONVERT(varchar(22),(SELECT start_lsn FROM cdc.change_tables WHERE capture_instance='lab_CdcProbe'),1) AS actual_low_watermark,
 (SELECT COUNT_BIG(*) FROM cdc.lab_CdcProbe_CT WHERE __$start_lsn<@effective) AS candidates_remaining
FOR JSON PATH,WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES;
"""
