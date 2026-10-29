"""Sparse, transactional derived images over the unchanged sales/v2 journal."""
import argparse
import json
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from resolve_core import VERSION,markers,reconstruct


def record_inputs(conn,events):
    from load import transform,canonical
    for event in events:
        if not markers(event.get('before')) and not markers(event.get('after')): continue
        row=transform(event,'0'*64)
        if row is None: continue
        conn.execute('''INSERT INTO business.resolution_input(event_id,contract_version,raw_before,raw_after)
            VALUES(%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING''',
            (row[0],VERSION,Jsonb(event.get('before'),dumps=canonical),Jsonb(event.get('after'),dumps=canonical)))


def rebuild_keys(conn,keys=None):
    from load import canonical,digest
    args=[];where=''
    if keys is not None:
        keys=sorted(set(keys))
        if not keys: return 0
        where=' WHERE (e.table_name,e.source_pk) IN ('+','.join('(%s,%s)' for _ in keys)+')'
        args=[item for pair in keys for item in pair]
    selected=conn.execute('''SELECT DISTINCT e.table_name,e.source_pk FROM business.resolution_input i
        JOIN staging.cdc_event e USING(event_id)'''+where,args).fetchall()
    changed=0
    with conn.cursor(row_factory=dict_row) as cur:
        for table,key in selected:
            history=cur.execute('''SELECT event_id,semantic_sha256,table_name,operation,row_data
                FROM staging.cdc_event WHERE table_name=%s AND source_pk=%s
                ORDER BY stream_phase,commit_lsn,change_lsn,event_serial_no LIMIT 100001''',(table,key)).fetchall()
            if len(history)>100000: raise ValueError('Resolution history exceeds the bounded per-key limit')
            inputs={r['event_id']:{'before':r['raw_before'],'after':r['raw_after']} for r in cur.execute('''
                SELECT i.* FROM business.resolution_input i JOIN staging.cdc_event e USING(event_id)
                WHERE e.table_name=%s AND e.source_pk=%s''',(table,key))}
            proofs={(r['event_id'],r['image'],r['column_name']):r for r in cur.execute('''
                SELECT p.* FROM business.marker_proof p JOIN staging.cdc_event e USING(event_id)
                WHERE e.table_name=%s AND e.source_pk=%s''',(table,key))}
            revision=digest({'version':VERSION,'events':[(h['event_id'],h['semantic_sha256']) for h in history],
                'proofs':sorted((list(k),p['proof_sha256']) for k,p in proofs.items())})
            for identity,result in reconstruct(history,inputs,proofs).items():
                values=[identity,VERSION,revision,result['status']]+[Jsonb(result[name],dumps=canonical)
                    for name in ['resolved_before','resolved_after','lineage','problems']]
                # Do not touch timestamps or revisions on an identical replay.
                changed+=cur.execute('''INSERT INTO business.event_resolution(event_id,contract_version,revision_sha256,
                    status,resolved_before,resolved_after,lineage,problems) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(event_id) DO UPDATE SET revision_sha256=excluded.revision_sha256,status=excluded.status,
                    resolved_before=excluded.resolved_before,resolved_after=excluded.resolved_after,
                    lineage=excluded.lineage,problems=excluded.problems,resolved_at=now()
                    WHERE business.event_resolution.revision_sha256<>excluded.revision_sha256''',values).rowcount
    return changed


def refresh_batch(conn,events,selected):
    record_inputs(conn,events)
    return rebuild_keys(conn,[(row[2],row[3]) for row in selected])


def main():
    from load import connect
    argparse.ArgumentParser(description=__doc__).parse_args()
    with connect() as conn,conn.transaction():
        conn.execute('SELECT pg_advisory_xact_lock(782641901)')
        changed=rebuild_keys(conn)
        print(json.dumps({'resolution_revisions_changed':changed,'purge_authorized':False}))


if __name__=='__main__': main()
