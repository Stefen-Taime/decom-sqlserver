"""Failure-path checks for the host maintenance script, with no Docker/source access."""
import importlib.util
import json
from pathlib import Path
from subprocess import CompletedProcess
import sys
import tempfile
import unittest
from unittest.mock import patch


class ResumeRunnerTests(unittest.TestCase):
    def test_capture_is_restarted_if_stop_or_state_capture_fails(self):
        path=Path('/host-scripts/verify-checkpoint-host.py')
        if not path.exists(): path=Path(__file__).resolve().parents[1]/'scripts/verify-checkpoint-host.py'
        spec=importlib.util.spec_from_file_location('resume_runner_under_test',path)
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        point={'checkpoint':{'commit_lsn':'00000001:00000001:0001','change_lsn':'00000001:00000001:0001',
                            'event_serial_no':1,'command_id':None}}
        for failure in ['stop','state']:
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);(root/'validation').mkdir();calls=[]
                def command(args,**kwargs):
                    calls.append(args)
                    if 'stop' in args and failure=='stop': return CompletedProcess(args,1,'','failed')
                    if 'verify_archive.py' in args: output=json.dumps({'operations':{'r':1}})
                    elif args[0]=='python3': output=json.dumps({'probe_id':'00000000-0000-0000-0000-000000000001'})
                    else: output=''
                    return CompletedProcess(args,0,output,'')
                # umask is process-wide: do not alter the test runner's own permissions.
                with patch.object(runner,'ROOT',root),patch.object(runner.os,'geteuid',return_value=0),\
                     patch.object(runner.os,'umask'),patch.object(runner.subprocess,'run',side_effect=command),\
                     patch.object(runner.checkpoint,'stable_state',return_value=(point,{})),\
                     patch.object(runner.checkpoint,'inspect',return_value={'state':{'Health':{'Status':'healthy'}}}),\
                     patch.object(runner.checkpoint,'state',side_effect=ValueError('invalid checkpoint')):
                    with self.assertRaises((ValueError,RuntimeError)): runner.main()
                recovery=[c for c in calls if c[:3]==['docker','compose','up']]
                self.assertEqual(len(recovery),1)
                self.assertEqual(recovery[0][-1],'debezium');self.assertIn('--no-deps',recovery[0])
                self.assertFalse((root/'validation/cdc-resume-v1-latest.json').exists())
                failure_reports=list((root/'validation').glob('*/failure.json'))
                self.assertEqual(len(failure_reports),1)
                self.assertFalse(json.loads(failure_reports[0].read_text())['passed'])


if __name__=='__main__': unittest.main()
