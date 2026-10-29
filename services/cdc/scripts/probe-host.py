"""Emit one isolated insert/update/delete cycle. Business tables stay untouched."""
import argparse
import json
import subprocess
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--id', default=None)
args = parser.parse_args()
probe_id = str(uuid.UUID(args.id)) if args.id else str(uuid.uuid4())
sql = f"""USE WideWorldImporters;
SET NOCOUNT ON;
INSERT lab.CdcProbe(ProbeID, Payload) VALUES ('{probe_id}', N'before');
UPDATE lab.CdcProbe SET Payload=N'after' WHERE ProbeID='{probe_id}';
DELETE lab.CdcProbe WHERE ProbeID='{probe_id}';
GO
"""
subprocess.run(['docker', 'exec', '-i', 'decom-sqlserver-sqlserver-1',
    '/bin/bash', '/opt/lab/bin/sqlcmd.sh'], input=sql, text=True, check=True)
print(json.dumps({'probe_id': probe_id, 'expected_operations': ['c', 'u', 'd']}))
