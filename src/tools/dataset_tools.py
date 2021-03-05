from ast import literal_eval

from src.settings import settings


def get_items_descriptions(x, items):
    """
    Returns a list of items description from a list of items ids
    """
    result = items.loc[[int(i) for i in x if i != 'None' and int(i) in items.index]].description.tolist()
    result = [str(j).replace("None", "") for j in result]
    return result


if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("/home/abdelouahab/PFE/lcm-tests/datasets/Total/transactions2.csv")
    df.columns = map(str.lower, df.columns)
    df = df.rename(columns={"cust_id": "customer_id"})
    df.to_sql("transactions", if_exists="replace", index=None, con=settings.engine)
