"""Load a verified reference snapshot atomically; retain every version and provenance."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('snapshot')
args=parser.parse_args()
reference=json.loads(Path(args.snapshot).read_text())
projection=['PackageTypeID','PackageTypeName','ValidFrom','ValidTo']
if reference['source_table']!='Warehouse.PackageTypes' or reference['projection']!=projection or not reference['rows']:
    raise ValueError('Unexpected reference contract')
data=json.dumps(reference['rows'],ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
checksum=hashlib.sha256(data).hexdigest()
if checksum!=reference['sha256'] or reference['version']!='wwi-package-types/v1/'+checksum:
    raise ValueError('Reference checksum/version mismatch')
keys=set()
for row in reference['rows']:
    if set(row)!=set(projection) or type(row['PackageTypeID']) is not int or row['PackageTypeID'] in keys:
        raise ValueError('Reference schema or key violation')
    keys.add(row['PackageTypeID'])
literal=json.dumps(reference,ensure_ascii=False).replace("'","''")
sql="""BEGIN;
CREATE TEMP TABLE reference_input(document jsonb) ON COMMIT DROP;
INSERT INTO reference_input VALUES ('"""+literal+"""'::jsonb);
GRANT SELECT ON reference_input TO warehouse_owner;
SET ROLE warehouse_owner;
SELECT pg_advisory_xact_lock(782641902);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM ops.reference_snapshots r JOIN reference_input i ON r.snapshot_version=i.document->>'version'
  WHERE r.payload_sha256<>i.document->>'sha256') THEN RAISE EXCEPTION 'Existing reference version conflicts'; END IF;
END $$;
INSERT INTO ops.reference_snapshots(snapshot_version,source_table,projection,payload_sha256,source_captured_at,row_count)
 SELECT document->>'version',document->>'source_table',document->'projection',document->>'sha256',
 (document->>'captured_at')::timestamptz,jsonb_array_length(document->'rows') FROM reference_input
 ON CONFLICT(snapshot_version) DO NOTHING;
INSERT INTO ref.package_types
 SELECT document->>'version',(r->>'PackageTypeID')::int,r->>'PackageTypeName',r->>'ValidFrom',r->>'ValidTo'
 FROM reference_input CROSS JOIN LATERAL jsonb_array_elements(document->'rows') r ON CONFLICT DO NOTHING;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM reference_input i WHERE (SELECT count(*) FROM ref.package_types p WHERE p.snapshot_version=i.document->>'version')<>jsonb_array_length(i.document->'rows'))
 OR EXISTS(SELECT 1 FROM reference_input i CROSS JOIN LATERAL jsonb_array_elements(i.document->'rows') r
  JOIN ref.package_types p ON p.snapshot_version=i.document->>'version' AND p.package_type_id=(r->>'PackageTypeID')::int
  WHERE (p.package_type_name,p.source_valid_from,p.source_valid_to) IS DISTINCT FROM (r->>'PackageTypeName',r->>'ValidFrom',r->>'ValidTo'))
 THEN RAISE EXCEPTION 'Existing reference rows differ from the verified snapshot'; END IF;
END $$;
COMMIT;
"""
result=subprocess.run(['docker','exec','-i','decom-warehouse-postgres-1','psql','-X','-q','-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse'],
    input=sql,text=True,capture_output=True)
if result.returncode:
    raise RuntimeError(next((line for line in result.stderr.splitlines() if 'ERROR:' in line),'Reference transaction failed'))
print(json.dumps({'reference_version':reference['version'],'rows':len(keys),'checksum':checksum,'mode':'explicit_versioned_snapshot','automatic_refresh':False}))
