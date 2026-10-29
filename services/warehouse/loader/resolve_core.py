"""Versioned reconstruction of unavailable values; raw events are never modified."""
from copy import deepcopy

VERSION='cdc-values/v1'
MARKER='__debezium_unavailable_value'
LOB_COLUMNS={
    'Sales.Orders':{'Comments','DeliveryInstructions','InternalComments'},
    'Sales.Invoices':{'CreditNoteReason','Comments','DeliveryInstructions','InternalComments','ReturnedDeliveryData'},
}


def markers(row):
    return sorted(name for name,value in (row or {}).items() if value==MARKER)


def reconstruct(history,inputs,proofs):
    """History is one key, ordered by source coordinates. Proofs attest marker semantics only.

    A source observation establishes an unchanged column, not its previous value. That
    value must independently come from this key's preceding, live, archived state.
    Explicit NULL is known; an absent field, unproved marker or deleted key is unknown.
    """
    state={};lineage={};unknown=set();live=False
    output={}
    for event in history:
        identity,table,operation=event['event_id'],event['table_name'],event['operation']
        raw=deepcopy(event['row_data'])
        envelope=inputs.get(identity)
        problems=[];links={};resolved={}
        if envelope:
            for image in ['before','after']:
                row=deepcopy(envelope[image])
                if row is None:
                    resolved[image]=None;continue
                for column in markers(row):
                    reason=None
                    proof=proofs.get((identity,image,column))
                    if operation!='u' or column not in LOB_COLUMNS.get(table,set()):
                        reason='marker_semantics_not_supported'
                    elif not proof or proof['semantic_sha256']!=event['semantic_sha256']:
                        reason='source_update_mask_proof_missing'
                    elif not live or column not in state or column in unknown:
                        reason='preceding_live_value_unknown'
                    if reason:
                        problems.append({'image':image,'column':column,'reason':reason})
                    else:
                        row[column]=deepcopy(state[column])
                        links[image+'.'+column]={'value_event_id':lineage[column],
                            'proof_sha256':proof['proof_sha256'],'source_observation_sha256':proof['source_observation_sha256']}
                resolved[image]=row
            output[identity]={'status':'blocked' if problems else 'resolved',
                'resolved_before':resolved['before'],'resolved_after':resolved['after'],
                'lineage':links,'problems':problems}
        if operation=='d':
            state={};lineage={};unknown=set();live=False
            continue
        # Only a snapshot/create establishes a lifecycle. A missing initial state cannot
        # be invented from an update when a subsequent marker needs historical proof.
        if operation in {'c','r'}: live=True
        next_state=deepcopy(resolved.get('after',raw));next_unknown=set(markers(next_state))
        # A literal marker in a create/snapshot is deliberately blocked for review too.
        next_lineage={name:identity for name in next_state}
        for name in markers(raw):
            key='after.'+name
            if key in links:
                next_lineage[name]=links[key]['value_event_id']
                next_unknown.discard(name)
        state,lineage,unknown=next_state,next_lineage,next_unknown
    return output
