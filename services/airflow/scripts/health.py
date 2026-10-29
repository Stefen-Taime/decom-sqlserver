"""Low-cost DAG processor heartbeat check without loading Airflow's CLI/provider graph."""
import os
from pathlib import Path
import psycopg2

url = Path('/run/secrets/sql_connection').read_text().strip().replace('postgresql+psycopg2://', 'postgresql://', 1)
with psycopg2.connect(url, connect_timeout=5) as connection:
    with connection.cursor() as cursor:
        cursor.execute("""SELECT state='running' AND latest_heartbeat>now()-interval '90 seconds'
                          FROM job WHERE job_type='DagProcessorJob' AND hostname=%s
                          ORDER BY latest_heartbeat DESC LIMIT 1""", (os.environ['HOSTNAME'],))
        row = cursor.fetchone()
        if not row or row[0] is not True:
            raise SystemExit('DAG processor heartbeat is missing or stale')
