import cProfile
import json
from itertools import groupby, product
from shutil import copyfile

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, MultiLabelBinarizer

from src.core.data_api.dataset_handler import DatasetHandler
from src.settings import settings
from src.settings.settings import STATS_FOLDER, GROUPS_DEMOGRAPHICS, SANKEY_TEMPLATE, LABELED_LINKS_FOLDER, \
    LINKS_FOLDER, GROUPS_FOLDER
from src.tools.dataset_tools import get_items_descriptions
from src.tools.demographics import extract_demographics
from src.tools.lcm_tools import read_lcm_output
from src.tools.sankey_tools import label_groups


def profileit(func):
    def wrapper(*args, **kwargs):
        datafn = func.__name__ + ".profile"  # Name the data file sensibly
        prof = cProfile.Profile()
        retval = prof.runcall(func, *args, **kwargs)
        prof.dump_stats(datafn)
        return retval

    return wrapper


class SankeyGenerator:

    def format_links(self, x):
        res = []
        for i in x[0]:
            for idx in range(len(i) - 1):
                res.append([i[idx], i[idx + 1], x["index"]])
        return res

    def make_links(self, e, index):
        """Product of groups ids for each two consecutive groups periods"""
        prev = e[0]
        for i in e[1:]:
            yield from product(prev, i, [index])
            prev = i

    def sankey_preprocessing(self, input_file, exp_name, user_apparition_threshold=0,
                             user_nunique_periods_threshold=1, keep_all_groups_in_periods=[]):

        le = LabelEncoder()
        demographics = extract_demographics(input_file)
        df = read_lcm_output(exp_name).sort_values("period").reset_index(drop=True)
        print("#Groups", df.shape[0])
        file = f'{LINKS_FOLDER}/{input_file}'
        mlb = MultiLabelBinarizer(sparse_output=True)
        df_users_apparition = mlb.fit_transform(df.user_ids.tolist()).astype(bool)
        df_users_apparition = pd.DataFrame(df_users_apparition.toarray(), columns=mlb.classes_)

        df_stats = df_users_apparition.sum()
        df_users_apparition = df_users_apparition[df_stats[df_stats > user_apparition_threshold].index]
        df_users_apparition = df_users_apparition.T.apply(lambda x: np.where(x)[0], axis=1)
        df_stats = df_users_apparition.to_frame()[0].apply(
            lambda x: list(list(z) for idx, z in groupby(x, lambda y: df.iloc[y].period))
        )

        df_stats = df_stats[df_stats.apply(lambda x: len(x)) > user_nunique_periods_threshold]
        res = []
        df_stats.to_frame().reset_index().apply(lambda x: [res.append(i) for i in self.make_links(x[0], x["index"])],
                                                axis=1)
        links = pd.DataFrame(res, columns=["source", "target", "user_id"])
        links = links.groupby(["source", "target"])["user_id"].apply(
            lambda x: ','.join(str(i) for i in x)).reset_index()
        if links.shape[0] == 0:
            print("No link found ")
            return
            # Keep groups appearing in at least one week
        groups_to_keep = np.unique(np.union1d(links.source.unique(), links.target.unique()))
        groups_to_keep = np.union1d(groups_to_keep, df[df.period.isin(keep_all_groups_in_periods)].index)
        df_groups = df.loc[groups_to_keep].dropna()
        df_groups['depth'] = le.fit_transform(df_groups.period) / df_groups.period.nunique()
        df_groups['size'] = df_groups.user_ids.apply(lambda x: len(x))
        if len(demographics) == 1:
            df_groups[demographics[0]] = df_groups.property_values
        else:
            df_groups[demographics] = df_groups.property_values.str.split("_", expand=True)
        links = self.make_labeled_links(input_file, links, df_groups)
        links["sankey_experiment_id"] = exp_name
        links.to_sql("Links", index=False, if_exists="replace", con=settings.engine)
        print("Done", input_file)

    def make_labeled_links(self, file_name, links, groups):
        links["user_id"] = links["user_id"].apply(lambda x: [int(i) for i in str(x).split(",")])
        links_extra = links.merge(groups[["user_ids"]], left_on="source", right_index=True) \
            .merge(groups[["user_ids"]], left_on="target", right_index=True)
        links_extra.columns = ["source", "target", "intersection", "source_users", "target_users"]
        links_extra["label"] = links_extra.apply(label_groups, axis=1)
        links_extra.columns = ['source', 'target', 'user_id', 'source_users', 'target_users', 'label']
        return links_extra[["source", "target", "user_id", "label"]]

    def generate_sankey_file(self, input_name):
        destination_file = SANKEY_TEMPLATE.replace("template", input_name.replace(".out", ""))
        copyfile(SANKEY_TEMPLATE, destination_file)
        return destination_file
