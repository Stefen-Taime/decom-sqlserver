"""Scoped archive verification and separate SQL-image evidence copy for the POC."""
from collections import Counter
from pathlib import Path
import hashlib
import json
import sys
sys.path.insert(0,'/app')
from archive import ArchiveStore,s3_client
from load import verified_batch,digest
from audit_core import archive_images,lsn
from row_values import canonical_row
from pilot_core import TABLE,row_key


def run():
    input_path=Path('/input/probe-source.json');data=input_path.read_bytes();source=json.loads(data)
    policy=json.loads(Path('/input/policy.json').read_text())
    run_id=json.loads(Path('/input/run.json').read_text())['run_id']
    client=s3_client();store=ArchiveStore(client,'cdc-raw',policy['archive_prefix'])
    expected={row_key(r):canonical_row(r['row_data'],source['captured_columns'][TABLE]) for r in source['tables'][TABLE]
              if lsn(r['commit_lsn'])>lsn(policy['snapshot_commit_lsn'])}
    if len(expected)!=sum(lsn(r['commit_lsn'])>lsn(policy['snapshot_commit_lsn']) for r in source['tables'][TABLE]):
        raise ValueError('Ambiguous source identities')
    observed={};semantic={};proofs=[]
    prefixes=[store.prefix+'/manifests/table='+TABLE+'/',store.prefix+'/manifests/table=_mixed/']
    keys=sorted({obj['Key'] for prefix in prefixes for page in client.get_paginator('list_objects_v2').paginate(
        Bucket=store.bucket,Prefix=prefix) for obj in page.get('Contents',[]) if obj['Key'].endswith('.receipt.json')})
    for key in keys:
        receipt,checksum,events=verified_batch(store,key);matched=0
        for event in events:
            src=event['source']
            if src['schema']+'.'+src['table']!=TABLE or event['op']=='r': continue
            rows=[event['before'],event['after']] if event['op']=='u' else [event['before'] if event['op']=='d' else event['after']]
            for identity,row in zip(archive_images(event,policy),rows):
                if identity not in expected: continue
                value=canonical_row(row,source['captured_columns'][TABLE]);signature=digest(event.get('before'))+digest(event.get('after'))
                if identity in observed and (observed[identity]!=value or semantic[identity]!=signature):
                    raise ValueError('Conflicting archived probe event')
                observed[identity]=value;semantic[identity]=signature;matched+=1
        if matched: proofs.append({'manifest_key':key,'manifest_sha256':checksum,'matched_images':matched})
    if observed!=expected: raise ValueError('Retained post-bootstrap probe images differ from their archive')
    # These are direct SQL CDC images, not synthesized Debezium events. They include
    # the twelve old technical images that predate the original connector bootstrap.
    key='control/purge-pilot/v1/'+run_id+'/source-images.json'
    sha=hashlib.sha256(data).hexdigest()
    existing=store._get(key)
    if existing is not None and existing!=data: raise ValueError('Evidence key collision')
    if existing is None:
        client.put_object(Bucket=store.bucket,Key=key,Body=data,ContentType='application/json',Metadata={'sha256':sha})
    if store._get(key)!=data: raise ValueError('Evidence readback differs')
    report={'contract':'cdc-probe-archive-proof/v1','source_observation_sha256':sha,
        'verified_manifests':len(keys),'matched_post_bootstrap_images':len(expected),'archive_proofs':proofs,
        'pre_bootstrap_images_preserved_as_sql_evidence':len(source['tables'][TABLE])-len(expected),
        'source_image_backup':{'bucket':store.bucket,'key':key,'sha256':sha,'images':len(source['tables'][TABLE]),
                             'readback_verified':True,'format':'sqlserver-cdc-probe-images/v1'},
        'production_purge_authorized':False}
    path=Path('/output/archive-proof.json');path.write_text(json.dumps(report,indent=2)+'\n');path.chmod(0o400)
    print(json.dumps({k:v for k,v in report.items() if k!='archive_proofs'}),flush=True)


if __name__=='__main__': run()
