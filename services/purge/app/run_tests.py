"""Run safety tests and emit the actual successful test count as machine-readable evidence."""
import json
from pathlib import Path
import sys
import unittest

directory=Path('/tests') if Path('/tests').is_dir() else Path(__file__).resolve().parents[1]/'tests'
suite=unittest.defaultTestLoader.discover(str(directory))
result=unittest.TextTestRunner(verbosity=2,stream=sys.stderr).run(suite)
print(json.dumps({'tests_run':result.testsRun,'failures':len(result.failures),
                  'errors':len(result.errors),'skipped':len(result.skipped),'successful':result.wasSuccessful()}))
sys.exit(0 if result.wasSuccessful() and result.testsRun>0 and not result.skipped else 1)
