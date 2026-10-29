"""Read-only archive/source proof preparation and independent derived-image comparison."""
import argparse
from collections import defaultdict
from functools import lru_cache
import hashlib
import json
from pathlib import Path
from psycopg.rows import dict_row
from archive import ArchiveStore,s3_client
from audit_core import archive_images,image_key
from row_values import canonical,canonical_row,explained_placeholder
from load import connect,verified_batch,transform,digest
from resolve_core import LOB_COLUMNS,markers


def source_index(source):
    rows=defaultdict(list)
    for table,images in source['tables'].items():
        for row in images:
            rows[image_key(table,row['pk'],row['operation'],row['commit_lsn'],row['change_lsn'])].append(row)
    return rows


def assess_image(captured,archived,columns,table,operation):
    """Return exact representations only if every difference is an evidenced marker."""
    left=dict(canonical_row(captured['row_data'],columns));right=dict(canonical_row(archived,columns))
    by_name={c['name']:c for c in columns};names=set(markers(archived))
    differences={name for name in left if left[name]!=right[name]}
    if not names or names!=differences or any(name not in LOB_COLUMNS.get(table,set()) or not explained_placeholder(
        left[name],right[name],by_name[name],operation,captured.get('update_mask')) for name in names):
        raise ValueError('Source differences are not entirely explained by unchanged LOB markers')
    return left,names


def compare_derived(captured,resolved,marker_names,origins):
    """Unchanged-marker cells use the proven old value, not CDC's unavailable NULL."""
    if set(captured)!=set(resolved) or set(origins)!=set(marker_names):
        raise ValueError('Derived column set or lineage is incomplete')
    if any(resolved[name]!=(origins[name] if name in marker_names else captured[name]) for name in captured):
        raise ValueError('Derived value differs from source or archived value provenance')
    return captured==resolved


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--verify',action='store_true');args=parser.parse_args()
    source_bytes=Path('/input/source.json').read_bytes();source=json.loads(source_bytes)
    source_hash=hashlib.sha256(source_bytes).hexdigest()
    policy=json.loads(Path('/input/policy.json').read_text());index=source_index(source)
    store=ArchiveStore(s3_client(),policy['archive_bucket'],policy['archive_prefix'])
    proofs=[];problems=[];checked_images=0;matched_images=0;cells=0
    literal_matches=0;lineage_cells=0;source_origin_cells=0
    @lru_cache(maxsize=8)
    def read_archive(key): return verified_batch(store,key)
    with connect() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        targets=cur.execute('''SELECT e.event_id,e.semantic_sha256,e.table_name,e.source_pk,
            e.stream_phase,e.commit_lsn,e.change_lsn,e.event_serial_no,i.raw_before,i.raw_after,
            a.manifest_key,a.manifest_sha256,r.status,r.resolved_before,r.resolved_after,r.lineage
            FROM business.resolution_input i JOIN staging.cdc_event e USING(event_id)
            JOIN ops.archive_batches a ON a.batch_id=e.first_batch_id
            LEFT JOIN business.event_resolution r USING(event_id) ORDER BY e.event_id LIMIT 10001''').fetchall()
        if len(targets)>10000: raise ValueError('Proof preparation exceeds the bounded event limit; partition the run')
        verified_manifests=set()
        for target in targets:
            manifest=target['manifest_key']
            receipt,checksum,events=read_archive(manifest);verified_manifests.add(manifest)
            if checksum!=target['manifest_sha256']: raise ValueError('Archive manifest differs from warehouse ledger')
            candidates=[event for event in events if (row:=transform(event,receipt['batch_id'])) and row[0]==target['event_id']]
            if not candidates: raise ValueError('Resolution event is absent from its verified archive')
            event=candidates[0];row=transform(event,receipt['batch_id'])
            if row[1]!=target['semantic_sha256'] or event.get('before')!=target['raw_before'] or event.get('after')!=target['raw_after']:
                raise ValueError('Archived event differs from the raw resolution input')
            if event['op']!='u':
                problems.append({'event_id':target['event_id'],'reason':'marker_is_not_a_supported_update'});continue
            table=target['table_name'];columns=source['captured_columns'][table];by_name={c['name']:c for c in columns}
            for image,key in zip(['before','after'],archive_images(event,policy)):
                if not markers(event[image]): continue
                rows=index.get(key,[])
                if len(rows)!=1:
                    problems.append({'event_id':target['event_id'],'image':image,'reason':'retained_source_identity_not_unique'});continue
                captured=rows[0]
                try: left,names=assess_image(captured,event[image],columns,table,key[2])
                except ValueError:
                    problems.append({'event_id':target['event_id'],'image':image,'reason':'source_differences_not_entirely_explained'});continue
                if args.verify:
                    if target['status']!='resolved':
                        problems.append({'event_id':target['event_id'],'image':image,'reason':'resolution_blocked'});continue
                    resolved=dict(canonical_row(target['resolved_'+image],columns));origins={}
                    for name in sorted(names):
                        link=target['lineage'].get(image+'.'+name)
                        if not link: raise ValueError('Derived marker lacks cell provenance')
                        proof=cur.execute('''SELECT * FROM business.marker_proof
                            WHERE event_id=%s AND image=%s AND column_name=%s''',(target['event_id'],image,name)).fetchone()
                        if not proof or proof['proof_sha256']!=link['proof_sha256'] or proof['semantic_sha256']!=target['semantic_sha256']:
                            raise ValueError('Derived marker proof differs from its source event')
                        if proof['source_observation_sha256']!=link['source_observation_sha256']:
                            raise ValueError('Derived marker observation differs from its proof')
                        origin=cur.execute('''SELECT e.*,a.manifest_key,a.manifest_sha256 FROM staging.cdc_event e
                            JOIN ops.archive_batches a ON a.batch_id=e.first_batch_id WHERE event_id=%s''',
                            (link['value_event_id'],)).fetchone()
                        position=lambda item:tuple(item[n] for n in ['stream_phase','commit_lsn','change_lsn','event_serial_no'])
                        if not origin or origin['table_name']!=table or origin['source_pk']!=target['source_pk'] or origin['operation']=='d' or position(origin)>=position(target):
                            raise ValueError('Value provenance is not an earlier live image of this source key')
                        old_receipt,old_checksum,old_events=read_archive(origin['manifest_key']);verified_manifests.add(origin['manifest_key'])
                        old=[e for e in old_events if (r:=transform(e,old_receipt['batch_id'])) and r[0]==origin['event_id']]
                        if not old or old_checksum!=origin['manifest_sha256'] or transform(old[0],old_receipt['batch_id'])[1]!=origin['semantic_sha256']:
                            raise ValueError('Original value event does not match its verified archive')
                        original_row=old[0]['after']
                        if name not in original_row or name in markers(original_row) or original_row!=origin['row_data']:
                            raise ValueError('Original value is missing, ambiguous or differs from the raw ledger')
                        origins[name]=canonical(original_row[name],by_name[name]);lineage_cells+=1
                        # If the earlier source image still exists, also compare the original
                        # archived value directly with SQL CDC. Snapshots may lack that image.
                        original_keys=archive_images(old[0],policy)
                        prior=index.get(original_keys[-1],[]) if original_keys else []
                        if len(prior)>1: raise ValueError('Original source image is ambiguous')
                        if prior:
                            if canonical(prior[0]['row_data'][name],by_name[name])!=origins[name]:
                                raise ValueError('Original archived value differs from retained SQL CDC')
                            source_origin_cells+=1
                    checked_images+=1;cells+=len(columns)
                    try:
                        literal_matches+=int(compare_derived(left,resolved,names,origins));matched_images+=1
                    except ValueError:
                        problems.append({'event_id':target['event_id'],'image':image,'reason':'derived_image_differs_from_source_or_lineage'})
                    continue
                for name in sorted(names):
                    evidence={'contract':'source-marker-proof/v1','source_observation_sha256':source_hash,
                        'capture_instance':policy['tables'][table]['capture_instance'],
                        'image_identity_sha256':digest(key),'source_image_sha256':digest(left),
                        'column':by_name[name],'update_mask':captured['update_mask'],'operation':key[2],
                        'manifest_key':manifest,'manifest_sha256':checksum,'source_is_null':True}
                    proofs.append({'event_id':target['event_id'],'image':image,'column_name':name,
                        'semantic_sha256':row[1],'source_observation_sha256':source_hash,
                        'proof_sha256':digest(evidence),'evidence':evidence})
    report={'contract':'source-marker-proof/v1','source_observation_sha256':source_hash,'proofs':proofs,
        'events_examined':len(targets),'verified_manifests':len(verified_manifests),'problems':problems,
        'derived_images_compared':checked_images,'derived_images_matching':matched_images,'derived_cells_compared':cells,
        'captured_representation_equal_images':literal_matches,'lineage_cells_verified':lineage_cells,
        'lineage_cells_also_matched_to_retained_source':source_origin_cells,
        'purge_authorized':False,'scope':'Available captured values plus independently verified original values for unchanged markers; not literal equality with unavailable CDC NULLs, a connector checkpoint or full snapshot reconciliation'}
    path=Path('/output/derived-comparison.json' if args.verify else '/output/marker-proofs.json')
    path.write_text(json.dumps(report,indent=2)+'\n');path.chmod(0o400)
    print(json.dumps({k:v for k,v in report.items() if k not in {'proofs','problems'}}|{'proof_count':len(proofs),'problem_count':len(problems)}))
    if problems: raise SystemExit(1)


if __name__=='__main__': main()
