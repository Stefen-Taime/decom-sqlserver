from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
if not Path('/app/resolve_core.py').exists():
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'warehouse/loader'))
from logical_values import LogicalValues,digest
from audit_core import archive_images,lsn
from row_values import RowValues,canonical_row
from resolve_core import MARKER,VERSION,reconstruct

TABLE='Sales.Orders';LOW='00000001:00000001:0001';HIGH='00000001:00000003:0001'
COLUMNS=[{'name':'OrderID','type':'int','ordinal':1},
         {'name':'Comments','type':'nvarchar','max_length':-1,'ordinal':2}]
POLICY={'snapshot_commit_lsn':LOW,'tables':{TABLE:{'primary_key':'OrderID','capture_instance':'Sales_Orders','warehouse_required':True}}}


def fixture():
    old={'op':'r','before':None,'after':{'OrderID':7,'Comments':'earlier text'},
         'source':{'schema':'Sales','table':'Orders','commit_lsn':LOW}}
    event={'op':'u','before':{'OrderID':7,'Comments':MARKER},'after':{'OrderID':7,'Comments':MARKER},
           'source':{'schema':'Sales','table':'Orders','commit_lsn':HIGH,'change_lsn':HIGH,'event_serial_no':2}}
    source={'upper_lsn':HIGH,'captured_columns':{TABLE:COLUMNS},'tables':{TABLE:[
        {'pk':'7','operation':op,'commit_lsn':HIGH,'change_lsn':HIGH,'update_mask':'0x01',
         'row_data':{'OrderID':'7','Comments':None}} for op in [3,4]]}}
    history=[{'event_id':ident,'semantic_sha256':digest(e),'table_name':TABLE,'operation':e['op'],'row_data':e['after']}
             for ident,e in [('old',old),('new',event)]]
    proofs=[]
    for image,key,captured in zip(['before','after'],archive_images(event,POLICY),source['tables'][TABLE]):
        evidence={'contract':'source-marker-proof/v1','source_observation_sha256':'a'*64,
            'capture_instance':'Sales_Orders','image_identity_sha256':digest(key),
            'source_image_sha256':digest(dict(canonical_row(captured['row_data'],COLUMNS))),
            'column':COLUMNS[1],'update_mask':'0x01','operation':key[2],
            'manifest_key':'manifest','manifest_sha256':'b'*64,'source_is_null':True}
        proofs.append({'event_id':'new','image':image,'column_name':'Comments','semantic_sha256':digest(event),
                       'source_observation_sha256':'a'*64,'proof_sha256':digest(evidence),'evidence':evidence})
    proof_map={(p['event_id'],p['image'],p['column_name']):p for p in proofs}
    result=reconstruct(history,{'new':{'before':event['before'],'after':event['after']}},proof_map)['new']
    revision=digest({'version':VERSION,'events':[(h['event_id'],h['semantic_sha256']) for h in history],
                     'proofs':sorted((list(k),p['proof_sha256']) for k,p in proof_map.items())})
    bundle={'contract':'cdc-resolution-audit-input/v1','proofs':proofs,'targets':[
        {'event_id':'new','semantic_sha256':digest(event),'table_name':TABLE,'source_pk':7,
         'contract_version':VERSION,'derived_contract_version':VERSION,'raw_before':event['before'],'raw_after':event['after'],
         'manifest_key':'manifest','manifest_sha256':'b'*64,'revision_sha256':revision,**result}]}
    return source,bundle,old,event


def evaluate(source,bundle,old,event,extra=None):
    logical=LogicalValues(source,POLICY,bundle)
    for identity,e in [('old',old),*(extra or []),('new',event)]:
        if e is None: continue
        phase=0 if e['op']=='r' else 1
        pos=bytes.fromhex(lsn(e['source']['commit_lsn'])) if phase else bytes(10)
        row=(identity,digest(e),TABLE,7,e['op'],phase,pos,pos,2 if phase else 0)
        logical.observe(e,row,'manifest','b'*64)
    raw=RowValues(source,POLICY);raw.observe(event)
    return logical.report(raw.report(),{'matched':True})


class ReconstructionAuditTests(unittest.TestCase):
    def test_rebuilds_nonnull_values_without_equating_marker_to_null(self):
        source,bundle,old,event=fixture()
        report=evaluate(source,bundle,old,event)
        self.assertTrue(report['matched']);self.assertEqual(report['strict_raw_matching_images'],0)
        self.assertEqual(report['logical_matching_images'],2);self.assertEqual(report['marker_cells_revalidated'],2)

    def test_modified_derived_value_is_rejected(self):
        source,bundle,old,event=fixture();bundle['targets'][0]['resolved_before']['Comments']=None
        with self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_wrong_origin_or_revision_is_rejected(self):
        for field in ['lineage','revision']:
            source,bundle,old,event=fixture()
            if field=='lineage': bundle['targets'][0]['lineage']['before.Comments']['value_event_id']='wrong'
            else: bundle['targets'][0]['revision_sha256']='0'*64
            with self.subTest(field=field),self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_missing_initial_state_is_rejected(self):
        source,bundle,old,event=fixture()
        with self.assertRaises(ValueError): evaluate(source,bundle,None,event)

    def test_intervening_update_or_delete_is_not_skipped(self):
        for operation in ['u','d']:
            source,bundle,old,event=fixture();middle=deepcopy(event)
            middle['op']=operation;middle['source']['commit_lsn']='00000001:00000002:0001'
            middle['before']={'OrderID':7,'Comments':'earlier text'}
            middle['after']=None if operation=='d' else {'OrderID':7,'Comments':'new text'}
            with self.subTest(operation=operation),self.assertRaises(ValueError):
                evaluate(source,bundle,old,event,[('middle',middle)])

    def test_missing_or_forged_proof_is_rejected(self):
        for mutation in ['remove','hash','column','source_hash','manifest']:
            source,bundle,old,event=fixture();bundle=deepcopy(bundle)
            if mutation=='remove': bundle['proofs'].pop()
            elif mutation=='hash': bundle['proofs'][0]['proof_sha256']='0'*64
            else:
                evidence=bundle['proofs'][0]['evidence']
                if mutation=='column': evidence['column']['ordinal']=1
                elif mutation=='source_hash': evidence['source_image_sha256']='0'*64
                else: evidence['manifest_key']='elsewhere'
                bundle['proofs'][0]['proof_sha256']=digest(evidence)
            with self.subTest(mutation=mutation),self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_changed_mask_or_literal_marker_cannot_reuse_previous_proof(self):
        for mutation in ['mask','literal']:
            source,bundle,old,event=fixture();source=deepcopy(source)
            if mutation=='mask': source['tables'][TABLE][0]['update_mask']='0x03'
            else: source['tables'][TABLE][0]['row_data']['Comments']=MARKER
            with self.subTest(mutation=mutation),self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_ambiguous_source_image_is_rejected(self):
        source,bundle,old,event=fixture();source['tables'][TABLE].append(deepcopy(source['tables'][TABLE][0]))
        with self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_target_or_proof_duplicates_are_rejected(self):
        for name in ['targets','proofs']:
            source,bundle,old,event=fixture();bundle[name].append(deepcopy(bundle[name][0]))
            with self.subTest(name=name),self.assertRaises(ValueError): evaluate(source,bundle,old,event)

    def test_empty_bundle_does_not_explain_markers(self):
        source,bundle,old,event=fixture();bundle['targets']=[];bundle['proofs']=[]
        self.assertFalse(evaluate(source,bundle,old,event)['matched'])

    def test_exact_redelivery_does_not_change_history(self):
        source,bundle,old,event=fixture()
        self.assertTrue(evaluate(source,bundle,old,event,[('old',old)])['matched'])


if __name__=='__main__': unittest.main()
