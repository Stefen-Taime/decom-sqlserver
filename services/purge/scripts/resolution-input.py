"""VM operator: bounded read-only export of the sparse reconstruction evidence.

No grants are added to the audit service. Business values stay in its root-protected
per-run input directory; only counts and checksums enter the public audit report.
"""
import json
import subprocess


SQL="""
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='120s';
SET LOCAL lock_timeout='15s';
SET LOCAL application_name='cdc-purge-resolution-observer';
WITH targets AS (
 SELECT i.event_id,i.contract_version,i.raw_before,i.raw_after,
        e.semantic_sha256,e.table_name,e.source_pk,a.manifest_key,a.manifest_sha256,
        r.contract_version AS derived_contract_version,
        r.status,r.revision_sha256,r.resolved_before,r.resolved_after,r.lineage,r.problems
 FROM business.resolution_input i JOIN staging.cdc_event e USING(event_id)
 JOIN ops.archive_batches a ON a.batch_id=e.first_batch_id
 LEFT JOIN business.event_resolution r USING(event_id)
 ORDER BY i.event_id LIMIT 10001
), proofs AS (
 SELECT event_id,image,column_name,semantic_sha256,proof_sha256,source_observation_sha256,evidence
 FROM business.marker_proof ORDER BY event_id,image,column_name LIMIT 100001
)
SELECT jsonb_build_object('contract','cdc-resolution-audit-input/v1',
 'targets',COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY event_id) FROM targets t),'[]'::jsonb),
 'proofs',COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY event_id,image,column_name) FROM proofs p),'[]'::jsonb));
COMMIT;
"""


def collect_resolution():
    result=subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-A','-t',
        '-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],input=SQL,text=True,
        capture_output=True,check=True,timeout=150)
    bundle=json.loads(result.stdout)
    if len(bundle['targets'])>10000 or len(bundle['proofs'])>100000:
        raise ValueError('Reconstruction evidence exceeds the bounded export limit')
    return bundle
