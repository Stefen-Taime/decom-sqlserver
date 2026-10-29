import copy
from datetime import date
from decimal import Decimal
import unittest
import pyarrow as pa
from common import Stats, arrow_schema, load_contract, paths


def row():
    return dict(invoice_line_id=1,invoice_id=1,order_id=None,customer_id=1,customer_name='Test',
        bill_to_customer_id=1,bill_to_customer_name='Test',invoice_date=date(2026,10,28),
        is_credit_note=True,stock_item_id=1,description='Credit',quantity=-2,
        unit_price=None,tax_rate=Decimal('20.000'),tax_amount=Decimal('-0.40'),
        line_profit=Decimal('-0.01'),extended_price=Decimal('-2.40'))


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.contract = load_contract('invoice_details')
        self.schema = arrow_schema(self.contract)

    def table(self, rows):
        return pa.Table.from_pylist(rows,schema=self.schema)

    def test_exact_credit_and_optional_order(self):
        stats=Stats(self.contract); stats.add(self.table([row()]))
        self.assertEqual(stats.result()['sums']['extended_price'],'-2.40')
        self.assertEqual(stats.result()['null_counts']['order_id'],1)

    def test_duplicate_key_across_batches(self):
        stats=Stats(self.contract); stats.add(self.table([row()]))
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            stats.add(self.table([row()]))

    def test_large_integer_aggregate_keeps_precision(self):
        stats=Stats(self.contract)
        stats.sums['quantity']=10000000000000003
        self.assertEqual(stats.result()['sums']['quantity'],'10000000000000003')

    def test_orphan_join_rejected(self):
        value=row(); value['customer_name']=None
        with self.assertRaisesRegex(ValueError,'Required'):
            Stats(self.contract).add(self.table([value]))

    def test_decimal_never_rounded(self):
        value=row(); value['tax_amount']=Decimal('0.001')
        with self.assertRaises(pa.ArrowInvalid):
            self.table([value])

    def test_decimal_overflow(self):
        value=row(); value['tax_amount']=Decimal('10000000000000000.00')
        with self.assertRaises(pa.ArrowInvalid):
            self.table([value])

    def test_integer_overflow(self):
        value=row(); value['quantity']=2**31
        with self.assertRaises((pa.ArrowInvalid,OverflowError)):
            self.table([value])

    def test_path_traversal_rejected(self):
        with self.assertRaises(ValueError):
            paths(self.contract,'../../another-run')

    def test_field_order_is_contract(self):
        with self.assertRaisesRegex(ValueError,'Schema'):
            Stats(self.contract).add(self.table([row()]).select(list(reversed(self.schema.names))))


if __name__=='__main__':
    unittest.main()
