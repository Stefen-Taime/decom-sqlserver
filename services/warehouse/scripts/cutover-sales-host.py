"""Stop only the warehouse worker and preserve a protected pre-v2 recovery point."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess

os.umask(0o077)
service=Path(__file__).resolve().parents[1]
root=Path('/srv/decom-sqlserver')
directory=root/'releases/warehouse-sales-v2-20261028'
directory.mkdir(parents=True,exist_ok=True,mode=0o700)
report=directory/'pre-cutover.json'
if report.exists():
    raise SystemExit('Existing cutover recovery point; inspect it instead of replacing it')
subprocess.run(['docker','compose','stop','loader'],cwd=service,check=True)
base=['docker','exec','decom-warehouse-postgres-1']
def pg(sql):
    return subprocess.check_output(base+['psql','-X','-A','-t','-v','ON_ERROR_STOP=1','-U','warehouse_admin','-d','warehouse','-c',sql],text=True).strip()
fingerprint_query="""SELECT encode(sha256(convert_to(coalesce(string_agg(row_to_json(a)::text,E'\n' ORDER BY batch_id),''),'UTF8')),'hex') FROM ops.archive_batches a WHERE consumer_version='customers-orders/v1'"""
events_query="""SELECT encode(sha256(convert_to(coalesce(string_agg(event_id||semantic_sha256,E'\n' ORDER BY event_id),''),'UTF8')),'hex') FROM staging.cdc_event WHERE table_name IN ('Sales.Customers','Sales.Orders') AND first_batch_id IN (SELECT batch_id FROM ops.archive_batches WHERE consumer_version='customers-orders/v1')"""
backup=directory/'warehouse-pre-v2.dump'
with backup.open('xb') as output:
    subprocess.run(base+['pg_dump','-U','warehouse_admin','-d','warehouse','-Fc'],stdout=output,check=True)
    output.flush()
    os.fsync(output.fileno())
with backup.open('rb') as source:
    checksum=hashlib.file_digest(source,'sha256').hexdigest()
result={'created_at':datetime.now(timezone.utc).isoformat(),'backup':str(backup),'backup_sha256':checksum,
    'legacy_batch_fingerprint':pg(fingerprint_query),'legacy_event_fingerprint':pg(events_query),
    'legacy_batch_query':fingerprint_query,'legacy_event_query':events_query,
    'legacy_batches':int(pg('SELECT count(*) FROM ops.archive_batches')),
    'legacy_events':int(pg('SELECT count(*) FROM staging.cdc_event')),
    'old_image_id':subprocess.check_output(['docker','image','inspect','--format','{{.Id}}','decom-warehouse-loader:1'],text=True).strip(),
    'raw_offsets_reset':False,'purge_authorized':False}
report.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if not k.endswith('_query')}))
