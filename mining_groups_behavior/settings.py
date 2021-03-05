import os

from sqlalchemy import create_engine
from pathlib import Path

NB_THREADS = 8

BASE_DIR = Path(__file__).resolve().parent.parent

# Path
LCM_EXECUTABLE = os.path.join(BASE_DIR, "bin/lcm")

OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')
TMP_FOLDER = os.path.abspath(f"{OUTPUT_FOLDER}/tmp")
RESULTS_FOLDER = os.path.abspath(f"{OUTPUT_FOLDER}/results")
STATS_FOLDER = os.path.abspath(f'{OUTPUT_FOLDER}/plots/stats')
LINKS_FOLDER = os.path.abspath(f'{OUTPUT_FOLDER}/plots/links')
LABELED_LINKS_FOLDER = os.path.abspath(f'{OUTPUT_FOLDER}/plots/labeled_links')
GROUPS_FOLDER = os.path.abspath(f'{OUTPUT_FOLDER}/plots/groups')
SANKEY_FOLDER = os.path.abspath(f'{OUTPUT_FOLDER}/plots/html')

SANKEY_TEMPLATE = f"{SANKEY_FOLDER}/template.html"

# Db requirements
TRANSACTIONS_TABLE_FIELDS = ['item_id', 'customer_id', 'station_id', 'transaction_date', 'transaction_id']
CUSTOMERS_TABLE_FIELDS = ["customer_id", "sex", "age", "departement"]
ITEMS_TABLE_FIELDS = ["description", "item_id"]

GROUPS_DEMOGRAPHICS = ["STATION_MGT_TYPE", "DEPARTEMENT"]

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://miningAgent:@localhost/QeNoBi")
engine = create_engine(DATABASE_URL)
LINKS_TABLE = "Links"
