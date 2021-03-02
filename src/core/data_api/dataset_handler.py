import pandas as pd
from sqlalchemy import create_engine


class DatasetHandler:
    def __init__(self):
        self.engine = create_engine("postgresql://miningAgent:@localhost/QeNoBi")

    def split_dataset(input_file, frequency, dataset_filter):
        """Split initial dataset to multiple files according to frequency

        Yields
        ------
        str
            names of partitions files
        """
        df = pd.read_csv(input_file)
        #     df.query(dataset_filter,inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s").dt.to_period(frequency)
        df.set_index(["timestamp"], inplace=True)
        df.movieId = df.movieId.astype(str)

        for period, period_df in df.groupby(pd.Grouper(freq=frequency)):
            output = period_df.groupby("userId").movieId.apply(lambda x: " ".join(x)).to_frame().reset_index()
            output.movieId = output.userId.astype(str) + " " + output.movieId
            output.drop(["userId"], inplace=True, axis=1)
            output.to_csv(str(period), header=None, index=None)
            yield period

    def get_data(self):  # TODO use ORM instead of queries
        """ Load transaction from Database"""
        query = """
        Select * from "transactions" t 
        join "customers" c on c."customer_id"=t."customer_id"
        join "stations"  s on t."station_id"=s."station_id"
        """
        df = pd.read_sql(query, con=self.engine).drop_duplicates()
        df.index = pd.to_datetime(df.transaction_date)
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    def get_items(self):
        return pd.read_sql_table("items", con=self.engine).set_index("item_id")
