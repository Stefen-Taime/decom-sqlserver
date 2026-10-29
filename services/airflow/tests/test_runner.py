import importlib.util
from pathlib import Path
import unittest
import json
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

path=Path(__file__).resolve().parents[1]/'runner/runner.py'
spec=importlib.util.spec_from_file_location('runner',path)
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)


class RunnerBoundaryTests(unittest.TestCase):
    def test_known_request(self):
        value={'action':'submit','dataset':'invoice_details','request_id':'af1-'+'a'*40,'operation':'export','attempt':1}
        self.assertEqual(runner.validate(value),value)

    def test_arbitrary_action_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'purge','dataset':'invoice_details'})

    def test_extra_command_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'preflight','dataset':'invoice_details','command':'id'})

    def test_unknown_dataset_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'preflight','dataset':'Sales.Customers'})

    def test_path_traversal_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'status','dataset':'order_details',
            'request_id':'../../etc/passwd','operation':'export'})

    def test_orders_have_no_messaging_receipt(self):
        with self.assertRaises(ValueError): runner.validate({'action':'invoice_receipt','dataset':'order_details','request_id':'af1-'+'a'*40})

    def test_boolean_attempt_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'submit','dataset':'order_details',
            'request_id':'af1-'+'a'*40,'operation':'export','attempt':True})

    def test_unsupported_operation_rejected(self):
        with self.assertRaises(ValueError): runner.validate({'action':'status','dataset':'order_details',
            'request_id':'af1-'+'a'*40,'operation':'shell'})

    def created_job(self, active):
        request={'action':'submit','dataset':'invoice_details','request_id':'af1-'+'a'*40,'operation':'export','attempt':2}
        job={'status':'running','request_id':request['request_id'],'operation':'export','attempt':1,
             'container':'decom-af-export-'+'a'*40+'-a1'}
        with tempfile.TemporaryDirectory() as directory, patch.object(runner,'STATE',Path(directory)):
            (Path(directory)/'requests').mkdir()
            runner.state_file(request).write_text(json.dumps({'dataset':'invoice_details','request_id':request['request_id'],
                                                              'jobs':{'export':job}}))
            with patch.object(runner,'inspect_job',return_value={'State':{'Running':False,'Status':'created'}}), \
                 patch.object(runner,'command',return_value=SimpleNamespace(stdout=active)) as command:
                result=runner.job_request(request)
                return result,command.call_args_list,job

    def test_interrupted_create_restarts_same_container(self):
        result,calls,job=self.created_job('')
        self.assertEqual(result['attempt'],1)
        self.assertEqual(result['container'],job['container'])
        self.assertEqual(calls[-1].args[0],['docker','start',job['container']])

    def test_interrupted_create_respects_global_capacity(self):
        result,calls,job=self.created_job('another-managed-job')
        self.assertEqual(result,{'status':'busy'})
        self.assertEqual(len(calls),1)


if __name__=='__main__': unittest.main()
