"""Run the business reconciliation with the read-only warehouse login."""
import argparse
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('source_export')
parser.add_argument('--output',required=True)
parser.add_argument('--database',default='warehouse',choices=['warehouse','warehouse_restore'])
parser.add_argument('--sales',action='store_true',help='Five-table streaming baseline directory')
args=parser.parse_args()
service=Path(__file__).resolve().parents[1]
source=Path(args.source_export).resolve()
result=subprocess.run(['docker','compose','run','--rm','--no-deps','--user','0:0',
    '-e','PGUSER=warehouse_reader','-e','PGPASSWORD_FILE=/run/secrets/reader','-e','PGDATABASE='+args.database,
    '-v','/srv/decom-sqlserver/secrets/warehouse/reader_password:/run/secrets/reader:ro',
    '-v',str(source)+':/input/source:ro','backfill' if args.sales else 'loader','python','reconcile-sales.py' if args.sales else 'reconcile.py','/input/source'],
    cwd=service,text=True,capture_output=True)
if result.returncode:
    raise RuntimeError(result.stdout+result.stderr)
path=Path(args.output)
path.parent.mkdir(parents=True,exist_ok=True)
path.write_text(result.stdout)
path.chmod(0o600)
print(result.stdout)
