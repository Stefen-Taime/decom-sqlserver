import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

path=Path('/app/source_observer.py') if Path('/app/source_observer.py').exists() else Path(__file__).resolve().parents[1]/'scripts/source.py'
spec=importlib.util.spec_from_file_location('observer',path)
observer=importlib.util.module_from_spec(spec);spec.loader.exec_module(observer)
POLICY={'max_source_rows_per_table':100,'retention_hours':72,
        'tables':{'Sales.Orders':{'capture_instance':'Sales_Orders','primary_key':'OrderID'}}}
COLUMNS={'Sales.Orders':[{'name':'OrderID','type':'int'}]}


class SourceObservationTests(unittest.TestCase):
    def test_capture_alias_preserves_dotted_table_rows(self):
        with patch.object(observer,'query',return_value={'tables':{'Sales_Orders':[{'pk':'7'}]}}) as query:
            result=observer.collect(POLICY,columns=COLUMNS)
        self.assertEqual(result['tables']['Sales.Orders'],[{'pk':'7'}])
        self.assertIn('AS [Sales_Orders]',query.call_args.args[0])
        self.assertNotIn('AS [Sales.Orders]',query.call_args.args[0])

    def test_nested_json_is_rejected_instead_of_counted_as_empty(self):
        with patch.object(observer,'query',return_value={'tables':{'Sales':{'Orders':[{'pk':'7'}]}}}):
            with self.assertRaises(ValueError): observer.collect(POLICY,columns=COLUMNS)

    def test_exact_projections_preserve_money_time_and_native_spatial_bytes(self):
        self.assertIn(',2)',observer.column_expression({'name':'Value','type':'money'}))
        self.assertIn(',126)',observer.column_expression({'name':'Value','type':'datetime2'}))
        self.assertIn('.Serialize()',observer.column_expression({'name':'Value','type':'geography'}))
        self.assertIn('nvarchar(128)',observer.column_expression({'name':'Value','type':'decimal'}))
        for column in [{'name':'Value','type':'float'},{'name':'unsafe]','type':'int'}]:
            with self.assertRaises(ValueError): observer.column_expression(column)

    def test_row_json_keeps_nulls(self):
        with patch.object(observer,'query',return_value={'tables':{}}) as query:
            observer.collect(POLICY,columns=COLUMNS)
        self.assertIn('WITHOUT_ARRAY_WRAPPER,INCLUDE_NULL_VALUES)) AS row_data',query.call_args.args[0])

    def test_missing_capture_metadata_is_rejected(self):
        with patch.object(observer,'query',return_value={'columns':[]}):
            with self.assertRaises(ValueError): observer.captured_columns(POLICY)

    def test_wide_capture_query_has_short_physical_lines(self):
        columns={'Sales.Orders':[{'name':'Column'+str(i),'type':'nvarchar'} for i in range(200)]}
        with patch.object(observer,'query',return_value={'tables':{}}) as query:
            observer.collect(POLICY,columns=columns)
        sql=query.call_args.args[0]
        self.assertGreater(len(sql),4000)
        self.assertLess(max(map(len,sql.splitlines())),2000)
