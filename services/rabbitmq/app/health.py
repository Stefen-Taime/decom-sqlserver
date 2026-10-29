import json
from pathlib import Path
import sys
import time
value=json.loads(Path('/state/health.json').read_text())
sys.exit(0 if value['status']=='ok' and time.time()-value['checked_at']<90 else 1)
