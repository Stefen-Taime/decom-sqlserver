import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / 'dags/pipeline_client.py'
spec = importlib.util.spec_from_file_location('client', path)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


class RetryIdentityTests(unittest.TestCase):
    def test_retry_uses_same_identity(self):
        first = client.request_id('invoice_details', 'scheduled__2026-10-29T10:00:00Z')
        self.assertEqual(first, client.request_id('invoice_details', 'scheduled__2026-10-29T10:00:00Z'))
        self.assertRegex(first, r'^af1-[a-f0-9]{40}$')

    def test_new_run_and_dataset_are_independent(self):
        identities = {client.request_id(dataset, run)
                      for dataset in ['invoice_details', 'order_details']
                      for run in ['manual__one', 'manual__two']}
        self.assertEqual(len(identities), 4)
