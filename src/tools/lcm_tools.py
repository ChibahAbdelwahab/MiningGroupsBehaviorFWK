import pandas as pd

from settings.settings import RESULTS_FOLDER


def read_lcm_output(input_name, columns=["user_ids", "support", "itemsets", "period", "property_values"]):
    """Read and restructure LCM output file,rename columns output a df """
    file = f'{RESULTS_FOLDER}/{input_name}'
    try:
        df = pd.read_csv(file, header=None)
    except:
        df = pd.DataFrame(columns=columns)
    df.columns = columns
    df["period"] = pd.to_datetime(df["period"])
    df["user_ids"] = df.user_ids.apply(lambda x: [int(z.replace('"', "")) for z in x[1:-1].split(",") if z != ""])
    return df
