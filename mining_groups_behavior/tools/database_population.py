from datetime import datetime
from datetime import timedelta
from random import randint, choice

import pandas as pd
from sqlalchemy import create_engine

from settings.settings import CUSTOMERS_TABLE_FIELDS, ITEMS_TABLE_FIELDS, TRANSACTIONS_TABLE_FIELDS

from mining_groups_behavior.settings import DATABASE_URL

engine = create_engine(DATABASE_URL)

### Generate transactions
nb_transactions = 1000
customers_range = (0, 1000)
articles_range = (0, 1000)
stations_range = (0, 1000)
date_min = datetime(2017, 1, 1)
max_days = 356
res = []
for i in range(0, nb_transactions):
    res.append((
        i,
        date_min + timedelta(days=randint(0, max_days)),
        randint(*articles_range),
        randint(*stations_range),
        randint(*customers_range)
    ))
transactions = pd.DataFrame(res, columns=TRANSACTIONS_TABLE_FIELDS)
transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"])
transactions.to_sql("transactions", con=engine, if_exists="replace", index=False)

sex_values = ["M", "F"]
age_values = ["<35", "35-49", ">49"]
departements_values = [i for i in range(0, 98)]
res = []
for i in transactions.customer_id.unique():
    res.append((
        i,
        choice(sex_values),
        choice(age_values),
        choice(departements_values)
    ))
customers = pd.DataFrame(res, columns=CUSTOMERS_TABLE_FIELDS)
customers.to_sql("customers", con=engine, if_exists="replace", index=False)
### Insertion of articles
items_columns = ["description", "item_id"]
res = []
for i in transactions.item_id.unique():
    res.append((
        f"I-{i}",
        i,
    ))
items_df = pd.DataFrame(res, columns=ITEMS_TABLE_FIELDS)
items_df.to_sql('items', con=engine, if_exists="replace", index=False)

#### Insertion of stations
stations_columns = ["station_id", "station_type"]
res = []
for i in transactions.station_id.unique():
    res.append((
        i,
        f"TYPE-{i % 3}"
    ))
stations = pd.DataFrame(res, columns=stations_columns)
stations.to_sql("stations", con=engine, if_exists="replace", index=False)
# %%


# %%
