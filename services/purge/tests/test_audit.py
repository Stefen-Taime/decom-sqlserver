from collections import Counter
import copy
import importlib.util
from pathlib import Path
import unittest

path=Path('/app/audit_core.py') if Path('/app/audit_core.py').exists() else Path(__file__).resolve().parents[1]/'app/audit_core.py'
spec=importlib.util.spec_from_file_location('core',path)
core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
COMMIT='00000001:00000020:0001'
CHANGE='00000001:00000010:0001'
POLICY={'tables':{'Sales.Orders':{'primary_key':'OrderID'}}}


def event(op):
    return {'op':op,'source':{'schema':'Sales','table':'Orders','commit_lsn':COMMIT,'change_lsn':CHANGE},
            'before':{'OrderID':7},'after':{'OrderID':7}}


class AuditSafetyTests(unittest.TestCase):
    def test_lsn_formats_are_equivalent(self):
        self.assertEqual(core.lsn(COMMIT),core.lsn('0x00000001000000200001'))

    def test_malformed_lsn_is_rejected(self):
        with self.assertRaises(ValueError): core.lsn('0x01')

    def test_missing_middle_with_same_endpoints_is_detected(self):
        rows=[core.image_key('Sales.Orders',i,2,COMMIT,CHANGE) for i in [1,2,3]]
        result=core.compare_images(Counter(rows),Counter([rows[0],rows[2]]))
        self.assertEqual(result['missing_row_images'],1)
        self.assertFalse(result['matched'])

    def test_multiplicity_is_not_collapsed(self):
        key=core.image_key('Sales.Orders',1,2,COMMIT,CHANGE)
        self.assertEqual(core.compare_images(Counter({key:2}),Counter({key:1}))['missing_row_images'],1)

    def test_update_has_two_source_images(self):
        self.assertEqual([k[2] for k in core.archive_images(event('u'),POLICY)],[3,4])

    def test_missing_update_before_image_blocks_match(self):
        keys=core.archive_images(event('u'),POLICY)
        self.assertFalse(core.compare_images(Counter(keys),Counter(keys[1:]))['matched'])

    def test_insert_and_delete_mapping(self):
        self.assertEqual(core.archive_images(event('c'),POLICY)[0][2],2)
        self.assertEqual(core.archive_images(event('d'),POLICY)[0][2],1)

    def test_snapshot_does_not_replace_cdc_history(self):
        self.assertEqual(core.archive_images(event('r'),POLICY),[])

    def test_primary_key_mutation_needs_explicit_mapping(self):
        value=event('u');value['after']['OrderID']=9
        with self.assertRaises(ValueError): core.archive_images(value,POLICY)

    def test_matching_identities_are_reported(self):
        rows=Counter(core.archive_images(event('u'),POLICY))
        self.assertTrue(core.compare_images(rows,rows)['matched'])

    def test_all_checks_true_still_cannot_authorize_purge(self):
        result=core.decision({'integrity':True,'coverage':True})
        self.assertFalse(result['purge_authorized'])
        self.assertIsNone(result['executable_low_watermark'])
        self.assertEqual(result['changes_deleted'],0)

    def test_unknown_evidence_blocks(self):
        result=core.decision({'restore':None,'scope':False,'integrity':True})
        self.assertIn('restore',result['blockers'])
        self.assertIn('scope',result['blockers'])
        self.assertNotIn('integrity',result['blockers'])

    def test_archive_only_probe_does_not_require_warehouse_ack(self):
        policy={'tables':{'lab.CdcProbe':{'warehouse_required':False}}}
        self.assertEqual(core.warehouse_record_count([{'source':{'schema':'lab','table':'CdcProbe'}}],policy),0)

    def test_mixed_batch_requires_warehouse_ack_for_business_rows(self):
        policy={'tables':{'lab.CdcProbe':{'warehouse_required':False},'Sales.Orders':{'warehouse_required':True}}}
        records=[{'source':{'schema':'lab','table':'CdcProbe'}},{'source':{'schema':'Sales','table':'Orders'}}]
        self.assertEqual(core.warehouse_record_count(records,policy),1)


if __name__=='__main__': unittest.main()
