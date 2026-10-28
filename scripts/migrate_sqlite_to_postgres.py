"""Run only with the old gateway stopped and a SQLite online-backup snapshot."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from rpa_gateway.app import Settings
from rpa_gateway.database import Database
from rpa_gateway.migration import import_sqlite

parser=argparse.ArgumentParser()
parser.add_argument('--source',required=True)
args=parser.parse_args()
database=Database(Settings.from_env().database,pool_size=1)
try:
    database.initialize()
    print(json.dumps(import_sqlite(database,args.source),ensure_ascii=False,indent=2))
finally:
    database.close()
