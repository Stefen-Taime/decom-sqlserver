"""Replay the real affected batches and prove that raw events and derived revisions stay unchanged."""
import json
from archive import ArchiveStore,s3_client
from load import connect,verified_batch,apply_batch,digest


def state(conn):
    rows=conn.execute('SELECT event_id,status,revision_sha256,resolved_at::text FROM business.event_resolution ORDER BY event_id').fetchall()
    return digest(rows)


with connect() as conn:
    before=state(conn)
    keys=[r[0] for r in conn.execute('''SELECT DISTINCT a.manifest_key FROM business.resolution_input i
        JOIN staging.cdc_event e USING(event_id) JOIN ops.archive_batches a ON a.batch_id=e.first_batch_id''')]
    store=ArchiveStore(s3_client(),'cdc-raw')
    added=0
    for key in keys:
        receipt,checksum,events=verified_batch(store,key)
        added+=apply_batch(conn,receipt,key,checksum,events)
    after=state(conn)
    assert before==after and added==0,'Real archive replay changed an existing derived revision'
    statuses=conn.execute('SELECT table_name,status,events,reconstructed_cells FROM ops.value_resolution_status ORDER BY table_name,status').fetchall()
    print(json.dumps({'affected_batches_replayed':len(keys),'new_raw_events':added,'derived_revisions_unchanged':before==after,
        'revision_inventory_sha256':after,'statuses':statuses,'purge_authorized':False}))
