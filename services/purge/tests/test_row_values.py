import base64
import copy
from decimal import Decimal, localcontext
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from row_values import canonical, canonical_row, RowValues, explained_placeholder, UNAVAILABLE

TABLE='Sales.Orders'
COMMIT='00000001:00000020:0001'
CHANGE='00000001:00000010:0001'
POLICY={'snapshot_commit_lsn':'00000001:00000001:0001',
        'tables':{TABLE:{'primary_key':'OrderID'}}}
COLUMNS=[{'name':'OrderID','type':'int'},{'name':'Amount','type':'decimal'},
         {'name':'Edited','type':'datetime2'},{'name':'Comment','type':'nvarchar'}]
ROW={'OrderID':'7','Amount':'123.450','Edited':'2026-10-28T12:00:00.1234567','Comment':None}


def source(rows=None):
    return {'upper_lsn':COMMIT,'captured_columns':{TABLE:copy.deepcopy(COLUMNS)},
            'tables':{TABLE:rows if rows is not None else [source_row(ROW,2)]}}


def source_row(row,operation):
    return {'pk':row['OrderID'],'operation':operation,'commit_lsn':COMMIT,'change_lsn':CHANGE,
            'row_data':copy.deepcopy(row)}


def event(row=None,op='c',before=None):
    return {'op':op,'source':{'schema':'Sales','table':'Orders','commit_lsn':COMMIT,'change_lsn':CHANGE},
            'after':copy.deepcopy(ROW if row is None else row),'before':copy.deepcopy(before)}


class ExactValueTests(unittest.TestCase):
    def test_decimal_38_digits_is_not_rounded_by_context(self):
        value='123456789012345678901234567890123456.78'
        with localcontext() as context:
            context.prec=6
            self.assertEqual(canonical(value,{'type':'decimal'}),value)
            self.assertEqual(canonical(Decimal(value),{'type':'decimal'}),value)

    def test_equivalent_decimal_encodings_match(self):
        for value in ['123.4500','1.2345E2',Decimal('123.45')]:
            self.assertEqual(canonical(value,{'type':'decimal'}),'123.45')
        self.assertEqual(canonical('-0.000',{'type':'decimal'}),'0')

    def test_decimal_float_and_nonfinite_values_are_rejected(self):
        for value in [123.45,True,'NaN','Infinity','invalid']:
            with self.subTest(value=value),self.assertRaises(ValueError): canonical(value,{'type':'decimal'})

    def test_large_integer_preserves_every_digit(self):
        value=9223372036854775807
        self.assertEqual(canonical(value,{'type':'bigint'}),str(value))
        for bad in [float(value),True,'7.0']:
            with self.subTest(value=bad),self.assertRaises(ValueError): canonical(bad,{'type':'bigint'})

    def test_timestamp_equivalence_preserves_seven_digits_and_year_9999(self):
        column={'type':'datetime2'}
        self.assertEqual(canonical('2026-10-28T12:00:00.123Z',column),
                         canonical('2026-10-28T12:00:00.1230000',column))
        self.assertEqual(canonical('9999-12-31T23:59:59.9999999Z',column),'9999-12-31T23:59:59.9999999')
        self.assertNotEqual(canonical('2026-10-28T12:00:00.1234567Z',column),
                            canonical('2026-10-28T12:00:00.1234566Z',column))

    def test_date_and_time_precision(self):
        self.assertEqual(canonical('2013-01-01Z',{'type':'date'}),'2013-01-01')
        self.assertEqual(canonical('12:01:02.1Z',{'type':'time'}),'12:01:02.1000000')

    def test_invalid_dates_offsets_and_excess_precision_are_rejected(self):
        for value in ['2026-02-30T12:00:00','2026-10-28T24:00:00','2026-10-28T12:00:00+01:00',
                      '2026-10-28T12:00:00.12345678Z']:
            with self.subTest(value=value),self.assertRaises(ValueError): canonical(value,{'type':'datetime2'})

    def test_text_is_not_trimmed_or_unicode_normalized(self):
        column={'type':'nvarchar'}
        self.assertNotEqual(canonical('é',column),canonical('e\u0301',column))
        self.assertNotEqual(canonical('abc ',column),canonical('abc',column))
        self.assertNotEqual(canonical('',column),canonical(None,column))

    def test_uuid_case_is_equivalent(self):
        value='6f4c55f7-8b84-499a-8c0a-cb25a534fe1e'
        self.assertEqual(canonical(value.upper(),{'type':'uniqueidentifier'}),value)

    def test_binary_comparison_preserves_all_native_spatial_bytes(self):
        value=base64.b64decode('5hAAAAEMgy6GF+m+R0B3bjVGRpBewA==')
        changed=bytes([value[0]^1])+value[1:]
        for kind in ['varbinary','geography','geometry']:
            column={'type':kind}
            self.assertEqual(canonical(base64.b64encode(value).decode(),column),value.hex())
            self.assertNotEqual(canonical(base64.b64encode(value).decode(),column),
                                canonical(base64.b64encode(changed).decode(),column))
            with self.assertRaises(ValueError): canonical('not base64!',column)

    def test_unknown_type_is_rejected_even_when_null(self):
        with self.assertRaises(ValueError): canonical(None,{'type':'new_sql_type'})

    def test_null_is_not_a_missing_column(self):
        missing=copy.deepcopy(ROW);del missing['Comment']
        with self.assertRaises(ValueError): canonical_row(missing,COLUMNS)
        self.assertEqual(len(canonical_row(ROW,COLUMNS)),4)

    def test_bit_is_not_a_general_truthy_value(self):
        self.assertEqual(canonical('1',{'type':'bit'}),True)
        self.assertEqual(canonical(False,{'type':'bit'}),False)
        with self.assertRaises(ValueError): canonical('false',{'type':'bit'})


class RowComparisonTests(unittest.TestCase):
    def test_unchanged_lob_placeholder_is_explained_but_never_certified(self):
        observation=source([source_row(ROW,3),source_row(ROW,4)])
        observation['captured_columns'][TABLE][-1].update(max_length=-1,ordinal=4)
        for row in observation['tables'][TABLE]: row['update_mask']='0x02'
        comparison=RowValues(observation,POLICY)
        marked={**ROW,'Comment':UNAVAILABLE}
        comparison.observe(event(marked,'u',marked))
        report=comparison.report()
        self.assertFalse(report['matched'])
        self.assertTrue(report['connector_representation_explains_all_differences'])
        self.assertEqual(report['explained_placeholder_cells'],2)
        self.assertEqual(report['placeholder_only_mismatched_images'],2)
        self.assertEqual(report['unexplained_mismatched_images'],0)

    def test_placeholder_diagnostic_cannot_hide_another_corrupt_value(self):
        observation=source([source_row(ROW,4)])
        observation['captured_columns'][TABLE][-1].update(max_length=-1,ordinal=4)
        observation['tables'][TABLE][0]['update_mask']='0x02'
        comparison=RowValues(observation,POLICY)
        comparison.observe(event({**ROW,'Comment':UNAVAILABLE,'Amount':'123.46'},'u',ROW))
        self.assertFalse(comparison.report()['matched'])
        self.assertFalse(comparison.report()['connector_representation_explains_all_differences'])
        self.assertEqual(comparison.report()['tables'][TABLE]['unexplained_differing_columns'],['Amount'])

    def test_equivalent_full_image_matches(self):
        row={**ROW,'OrderID':7,'Amount':'123.45','Edited':ROW['Edited']+'Z'}
        comparison=RowValues(source(),POLICY);comparison.observe(event(row))
        self.assertTrue(comparison.report()['matched'])
        self.assertEqual(comparison.report()['compared_cells'],4)

    def test_one_cent_change_with_identical_identity_is_detected(self):
        comparison=RowValues(source(),POLICY);comparison.observe(event({**ROW,'Amount':'123.46'}))
        report=comparison.report()
        self.assertFalse(report['matched'])
        self.assertEqual(report['tables'][TABLE]['differing_columns'],['Amount'])
        self.assertEqual(report['tables'][TABLE]['mismatched_images'],1)

    def test_one_hundred_nanosecond_change_is_detected(self):
        comparison=RowValues(source(),POLICY)
        comparison.observe(event({**ROW,'Edited':'2026-10-28T12:00:00.1234566Z'}))
        self.assertFalse(comparison.report()['matched'])
        self.assertEqual(comparison.report()['tables'][TABLE]['differing_columns'],['Edited'])

    def test_missing_and_additional_archive_columns_block_match(self):
        missing=copy.deepcopy(ROW);del missing['Comment']
        for row in [missing,{**ROW,'Added':'unexpected'}]:
            comparison=RowValues(source(),POLICY);comparison.observe(event(row))
            self.assertFalse(comparison.report()['matched'])

    def test_update_compares_before_and_after(self):
        after={**ROW,'Amount':'124.00'}
        observation=source([source_row(ROW,3),source_row(after,4)])
        comparison=RowValues(observation,POLICY);comparison.observe(event(after,'u',ROW))
        self.assertEqual(comparison.report()['matching_images'],2)
        self.assertTrue(comparison.report()['matched'])
        comparison=RowValues(observation,POLICY)
        comparison.observe(event(after,'u',{**ROW,'Amount':'123.44'}))
        self.assertFalse(comparison.report()['matched'])
        self.assertEqual(comparison.report()['matching_images'],1)

    def test_delete_compares_before_image(self):
        comparison=RowValues(source([source_row(ROW,1)]),POLICY)
        comparison.observe(event(op='d',before=ROW))
        self.assertTrue(comparison.report()['matched'])

    def test_duplicate_image_multiplicity_is_not_collapsed(self):
        comparison=RowValues(source([source_row(ROW,2),source_row(ROW,2)]),POLICY)
        comparison.observe(event())
        self.assertFalse(comparison.report()['matched'])
        comparison.observe(event())
        self.assertTrue(comparison.report()['matched'])

    def test_empty_retained_table_is_explicitly_unexercised(self):
        comparison=RowValues(source([]),POLICY)
        comparison.snapshot_shape(event(op='r'))
        self.assertEqual(comparison.report()['tables'][TABLE]['status'],'no_retained_images')
        self.assertEqual(comparison.report()['compared_cells'],0)

    def test_snapshot_shape_mismatch_blocks_even_without_retained_images(self):
        comparison=RowValues(source([]),POLICY)
        comparison.snapshot_shape(event({'OrderID':7},op='r'))
        self.assertFalse(comparison.report()['matched'])

    def test_pre_bootstrap_values_do_not_count_as_stream_coverage(self):
        row=source_row(ROW,2);row['commit_lsn']=POLICY['snapshot_commit_lsn']
        comparison=RowValues(source([row]),POLICY)
        self.assertEqual(comparison.report()['source_images'],0)

    def test_source_column_drift_fails_closed(self):
        observation=source();observation['captured_columns'][TABLE].append({'name':'Added','type':'int'})
        with self.assertRaises(ValueError): RowValues(observation,POLICY)


class PlaceholderDiagnosisTests(unittest.TestCase):
    COLUMN={'name':'LongText','type':'nvarchar','max_length':-1,'ordinal':9}

    def test_mask_uses_last_byte_low_bit_for_ordinal_one(self):
        self.assertTrue(explained_placeholder(None,UNAVAILABLE,self.COLUMN,3,'0x0001'))
        self.assertFalse(explained_placeholder(None,UNAVAILABLE,self.COLUMN,3,'0x0100'))

    def test_missing_short_or_invalid_mask_is_not_accepted(self):
        for mask in [None,'0x00','0x','0xz0']:
            self.assertFalse(explained_placeholder(None,UNAVAILABLE,self.COLUMN,3,mask))

    def test_not_a_general_null_marker_equivalence(self):
        for op in [1,2]:
            self.assertFalse(explained_placeholder(None,UNAVAILABLE,self.COLUMN,op,'0x0000'))
        self.assertFalse(explained_placeholder('value',UNAVAILABLE,self.COLUMN,3,'0x0000'))
        self.assertFalse(explained_placeholder(None,UNAVAILABLE,{**self.COLUMN,'max_length':200},3,'0x0000'))

    def test_binary_marker_is_diagnosed_after_decoding(self):
        column={**self.COLUMN,'type':'varbinary'}
        self.assertTrue(explained_placeholder(None,UNAVAILABLE.encode().hex(),column,4,'0x0000'))
