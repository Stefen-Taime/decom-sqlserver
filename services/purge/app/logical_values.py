"""Revalidate cdc-values/v1 against this audit's source and verified archive scan.

The protected warehouse bundle is evidence to check, never an approval. Histories,
proof semantics, lineage and revisions are independently rebuilt from the archive.
"""
from collections import defaultdict
import hashlib
import json
import re

from audit_core import archive_images, image_key, lsn
from row_values import canonical_row, explained_placeholder
from resolve_core import VERSION, LOB_COLUMNS, markers, reconstruct


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


class LogicalValues:
    MAX_EVENTS=100000

    def __init__(self,source,policy,bundle):
        if bundle.get('contract')!='cdc-resolution-audit-input/v1':
            raise ValueError('Unsupported reconstruction input contract')
        if len(bundle['targets'])>10000 or len(bundle['proofs'])>100000:
            raise ValueError('Reconstruction input exceeds audit bounds')
        self.source,self.policy,self.bundle=source,policy,bundle
        self.targets={row['event_id']:row for row in bundle['targets']}
        if len(self.targets)!=len(bundle['targets']): raise ValueError('Duplicate reconstruction target')
        self.keys={(t['table_name'],str(t['source_pk'])) for t in self.targets.values()}
        self.history=defaultdict(dict);self.manifests=defaultdict(set);self.errors=[];self.event_count=0
        self.source_rows=defaultdict(list)
        for table,rows in source['tables'].items():
            for row in rows:
                self.source_rows[image_key(table,row['pk'],row['operation'],row['commit_lsn'],row['change_lsn'])].append(row)

    def wants(self,event):
        table=event['source']['schema']+'.'+event['source']['table']
        if not self.policy['tables'][table]['warehouse_required']: return False
        row=event['before'] if event['op']=='d' else event['after']
        return (table,str(row[self.policy['tables'][table]['primary_key']])) in self.keys or bool(
            markers(event.get('before')) or markers(event.get('after')))

    def observe(self,event,transformed,manifest,checksum):
        identity,semantic,table,key,op,phase,commit,change,serial=transformed[:9]
        key=(table,str(key))
        if key not in self.keys:
            self.errors.append('archived_marker_missing_from_warehouse_bundle');return
        history=self.history[key]
        item={'event_id':identity,'semantic_sha256':semantic,'table_name':table,'operation':op,
              'row_data':event['before'] if op=='d' else event['after'],
              'position':(phase,commit.hex(),change.hex(),serial),'envelope':event}
        if identity in history:
            # Delivery clocks and batch boundaries are not part of event semantics.
            if history[identity]['semantic_sha256']!=semantic:
                raise ValueError('Conflicting event in reconstruction history')
        else:
            history[identity]=item;self.event_count+=1
        self.manifests[identity].add((manifest,checksum))
        if self.event_count>self.MAX_EVENTS:
            raise ValueError('Reconstruction audit exceeds the total history limit')

    def validate_proof(self,proof,item,image,column,captured,canonical_source,key):
        evidence=proof['evidence'];table=item['table_name']
        expected={'contract':'source-marker-proof/v1',
            'source_observation_sha256':proof['source_observation_sha256'],
            'capture_instance':self.policy['tables'][table]['capture_instance'],
            'image_identity_sha256':digest(key),'source_image_sha256':digest(canonical_source),
            'column':column,'update_mask':captured['update_mask'],'operation':key[2],
            'manifest_key':evidence.get('manifest_key'),'manifest_sha256':evidence.get('manifest_sha256'),
            'source_is_null':True}
        if (proof['semantic_sha256']!=item['semantic_sha256'] or evidence!=expected
                or not re.fullmatch('[0-9a-f]{64}',proof['source_observation_sha256'])
                or proof['proof_sha256']!=digest(evidence)
                or (evidence['manifest_key'],evidence['manifest_sha256']) not in self.manifests[item['event_id']]):
            raise ValueError('Marker proof does not match this source image and verified archive')

    def report(self,raw,coverage):
        proofs={(p['event_id'],p['image'],p['column_name']):p for p in self.bundle['proofs']}
        if len(proofs)!=len(self.bundle['proofs']): raise ValueError('Duplicate marker proof')
        examined=set();used_proofs=set();images=0;cells=0
        for key in sorted(self.keys):
            history=sorted(self.history[key].values(),key=lambda item:item['position'])
            if len({item['position'] for item in history})!=len(history):
                raise ValueError('Affected key has ambiguous source ordering')
            inputs={};key_proofs={};image_count=0;cell_count=0
            for item in history:
                identity=item['event_id'];event=item['envelope']
                marked=markers(event.get('before')) or markers(event.get('after'))
                if not marked: continue
                target=self.targets.get(identity)
                if not target: raise ValueError('History has an unregistered marker event')
                if target['contract_version']!=VERSION or target['derived_contract_version']!=VERSION or target['semantic_sha256']!=item['semantic_sha256']:
                    raise ValueError('Warehouse reconstruction contract or semantic hash differs')
                if target['raw_before']!=event.get('before') or target['raw_after']!=event.get('after'):
                    raise ValueError('Warehouse reconstruction input differs from archive')
                if (target['manifest_key'],target['manifest_sha256']) not in self.manifests[identity]:
                    raise ValueError('Warehouse target manifest is absent from verified scan')
                if event['op']!='u' or not lsn(self.policy['snapshot_commit_lsn'])<item['position'][1]<=lsn(self.source['upper_lsn']):
                    raise ValueError('Marker is outside the supported retained streaming interval')
                examined.add(identity);inputs[identity]={'before':event['before'],'after':event['after']}
                columns=self.source['captured_columns'][item['table_name']];by_name={c['name']:c for c in columns}
                for image,source_key in zip(['before','after'],archive_images(event,self.policy)):
                    names=set(markers(event[image]))
                    if not names: continue
                    source_rows=self.source_rows[source_key]
                    if len(source_rows)!=1: raise ValueError('Marker source identity missing or ambiguous')
                    captured=source_rows[0]
                    left=dict(canonical_row(captured['row_data'],columns));right=dict(canonical_row(event[image],columns))
                    differences={name for name in left if left[name]!=right[name]}
                    if names!=differences: raise ValueError('Other captured values differ or marker is literal')
                    for name in sorted(names):
                        if name not in LOB_COLUMNS.get(item['table_name'],set()) or not explained_placeholder(
                                left[name],right[name],by_name[name],source_key[2],captured.get('update_mask')):
                            raise ValueError('Current source observation does not prove an unchanged LOB')
                        proof_key=(identity,image,name);proof=proofs.get(proof_key)
                        if not proof: raise ValueError('Marker proof missing')
                        self.validate_proof(proof,item,image,by_name[name],captured,left,source_key)
                        key_proofs[proof_key]=proof;used_proofs.add(proof_key);cell_count+=1
                    image_count+=1
            expected=reconstruct(history,inputs,key_proofs)
            revision=digest({'version':VERSION,'events':[(h['event_id'],h['semantic_sha256']) for h in history],
                'proofs':sorted((list(k),p['proof_sha256']) for k,p in key_proofs.items())})
            for identity,result in expected.items():
                target=self.targets[identity]
                if result['status']!='resolved' or any(target[name]!=value for name,value in result.items()):
                    raise ValueError('Derived values, lifecycle or provenance differ from archived history')
                if target['revision_sha256']!=revision:
                    raise ValueError('Derived revision differs from the complete verified key history')
            images+=image_count;cells+=cell_count
        if examined!=set(self.targets) or used_proofs!=set(proofs):
            raise ValueError('Unused or missing reconstruction targets/proofs')
        passed=(not self.errors and coverage['matched'] and raw['issue_count']==0
            and raw['connector_representation_explains_all_differences']
            and images==raw['placeholder_only_mismatched_images']
            and raw['source_images']==raw['archive_images']==raw['matching_images']+images)
        return {'contract':'sqlserver-cdc-logical-values/v1','matched':passed,
            'strict_raw_matching_images':raw['matching_images'],'reconstructed_images_verified':images,
            'logical_matching_images':raw['matching_images']+images,'marker_cells_revalidated':cells,
            'history_events_verified':sum(map(len,self.history.values())),
            'affected_keys':len(self.keys),'issues':self.errors[:20],
            'scope':'Retained images: exact available captured values and unchanged markers reconstructed from verified prior live archive state; not full snapshot reconciliation or transaction completeness'}
