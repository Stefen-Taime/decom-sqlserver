"""Example S3-only consumer: discover commits, validate every row, then persist an acknowledgement."""
import argparse
import json
import os
from pathlib import Path
from common import Store, atomic_json, load_contract, require, utcnow, verify_manifest


def consume(contract, publication):
    store = Store()
    marker = json.loads(store.get(publication))
    manifest = verify_manifest(store, contract, marker, publication)
    result = dict(dataset=contract['dataset'], run_id=manifest['run_id'], publication=publication,
                  manifest_sha256=marker['manifest_sha256'], statistics=manifest['statistics'],
                  files=len(manifest['files']), bytes=sum(f['bytes'] for f in manifest['files']),
                  verified_at=utcnow(), status='validated')
    # This demo has no business sink. A real application stages its replacement and commits atomically here.
    atomic_json(Path(os.environ['CONSUMER_STATE_DIR'])/(marker['manifest_sha256']+'.json'), result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['list','read'])
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--publication')
    args = parser.parse_args()
    contract = load_contract(args.dataset)
    if args.action == 'list':
        store = Store()
        require(store.bucket == contract['bucket'], 'Bucket differs from contract')
        prefix = f"{contract['publication_prefix']}/dataset={contract['dataset']}/version={contract['version']}/"
        print(json.dumps({'publications':list(store.list(prefix))}))
    else:
        require(args.publication, 'Provide a committed publication key')
        print(json.dumps(consume(contract, args.publication)))
