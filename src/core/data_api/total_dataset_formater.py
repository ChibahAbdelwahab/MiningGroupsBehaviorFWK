import json
import os

import pandas as pd
from sklearn import preprocessing


class TotalDatasetHandler:
    def concat_total_dataset(self, files_path='../datasets/Total/data'):
        df = pd.DataFrame()
        a = os.system(f"ls {files_path}")
        for i in a:
            df = pd.concat([pd.read_csv(f"../datasets/Total/data/{i}", sep=";"), df])
        df_users = pd.read_csv("../datasets/Total/users.csv", sep=";")
        df = df.merge(df_users, on="CUST_ID")
        df.index = pd.to_datetime(df.TRANSACTION_DATE).dt.to_period("D")
        df["STATION_DEPARTEMENT_ID"] = df.STATION_ID.apply(lambda x: x[3:5])
        df = df[df.STATION_DEPARTEMENT_ID == df.DEPARTEMENT]
        stations = pd.read_csv("../datasets/Total/STATION_TYPE.csv", sep=";")
        df = df.merge(stations, on="STATION_ID").dropna()
        df.index = pd.to_datetime(df.TRANSACTION_DATE).dt.to_period("D")
        df['date'] = pd.to_datetime(df.TRANSACTION_DATE)
        return df

    def get_data(self):
        # Concat all total dataset
        df = self.concat_total_dataset()
        df.ARTICLE_ID = self.preprocessing_similar_ids(df)
        # from sklearn import preprocessing
        le = preprocessing.LabelEncoder()
        df["ARTICLE_ID"] = le.fit_transform(df["ARTICLE_ID"])
        df["a"] = 1  # Must set a "grouping" attribute while we don't consider demographics

        df_copy = df.copy()

    def preprocessing_similar_ids(self, df, mapping_file_path="../datasets/Total/ids_mapping.json",
                                  attribute_name="ARTICLE_ID"):
        with open(mapping_file_path, "r") as f:
            mapping = json.load(f)
            ids = df[attribute_name].astype(str).replace(mapping)
        return ids
