import json
from database import Database

import warnings
warnings.filterwarnings("ignore")

with open("comp.json", "r", encoding="utf-8") as f:
    competitions_info = json.load(f)

print(competitions_info)
print()

database = Database(competitions_info)
database.update(db_name='fotcer', save_data_folder='data', save_db_folder=None)

print(database.check_all_tables())