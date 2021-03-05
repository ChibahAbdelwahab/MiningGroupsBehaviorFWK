from ast import literal_eval

import pandas as pd
from sqlalchemy import create_engine

from mining_groups_behavior.settings.settings import RESULTS_FOLDER, DATABASE_URL


def read_lcm_output(input_name, columns=["user_ids", "support", "itemsets", "period", "property_values"]):
    """Read and restructure LCM output file,rename columns output a df """
    engine = create_engine(DATABASE_URL)
    query = f"""
            Select * from "Groups" where sankey_experiment_id='{input_name}'
    """
    df = pd.read_sql(query, con=engine)
    df = df.rename(columns={"customer_id": "user_ids"})
    print(df.columns)

    df["period"] = pd.to_datetime(df["period"])
    df["user_ids"] = df.user_ids.apply(literal_eval)
    return df

    try:
        df = pd.read_csv(file, header=None)
    except:
        df = pd.DataFrame(columns=columns)
    df.columns = columns
    df["period"] = pd.to_datetime(df["period"])
    df["user_ids"] = df.user_ids.apply(lambda x: [int(z.replace('"', "")) for z in x[1:-1].split(",") if z != ""])
    return df
