import subprocess
import sys

import pandas as pd

from mining_groups_behavior.settings import *
from mining_groups_behavior.tools.dataset_tools import get_items_descriptions


class MiningHandler:
    """ This Class gives an api to use LCM algorithm """

    def __init__(self, dh=None):
        self.dh = dh
        self.engine = create_engine(DATABASE_URL)

    def dataset_property_split(self, df, frequency, properties, min_support, groupby_property="customer_id",
                               itemset_property="item_id", ):
        """
        Split dataset according to the given properties
        :param df: Dataframe
        :param frequency: D,W,M
        :param properties: list of str
            Ex: ["sex","Age"]
        :param min_support: int
        :param groupby_property: str
        :param itemset_property: str
        :param temp_folder
        :param indexation_folder:
        :return: file names generator
        """
        pass

    def reformat_output(self, raw_result, split_name):
        """
        Reformat default output of lcm  to a dataframe with structure : min_support,itemsets,users
        """
        pass

    def run_lcm(self, split_name, itemsets_size, support, output_file):
        """Runs LCM (Single Thread)  and return the  result formated with format_output
        Output:
            Replace file having name input_file with the result : support,itemset,users
            if no itemset found the input_file is deleted and output is empty string ""
        """
        pass

    def multithread_lcm(self, df, frequency, support, itemsets_size, properties, output_file):
        pass

    def format_output_name(self, frequency, min_support, itemsets_size, properties):
        return f'{RESULTS_FOLDER}/{frequency}-{min_support}-[{itemsets_size[0]}-{itemsets_size[1]}]-[{",".join(str(i) for i in properties)}]-lcm.out'


class LcmHandler(MiningHandler):
    """ This Class gives an api to use LCM algorithm """

    def dataset_property_split(self, df, frequency, properties, min_support, groupby_property="customer_id",
                               itemset_property="item_id", ):
        """
        Split dataset according to the given properties
        :param df: Dataframe
        :param frequency: D,W,M
        :param properties: list of str
            Ex: ["sex","Age"]
        :param min_support: int
        :param groupby_property: str
        :param itemset_property: str
        :param temp_folder
        :param indexation_folder:
        :return: file names generator
        """
        for period, i in df.groupby(pd.Grouper(freq=frequency)):
            for values, ii in i.groupby(properties):
                if len(properties) > 1:
                    values = '_'.join(str(z) for z in values)

                split_name = f"{TMP_FOLDER}/splits/{period}_{values}#{min_support}"
                index_file_name = f"{TMP_FOLDER}/index/{period}_{values}#{min_support}"
                ii = ii.groupby(groupby_property)[itemset_property].apply(lambda x: " ".join(str(z) for z in x))

                ii.to_csv(split_name, index=None, header=False)
                pd.DataFrame(ii.index).to_csv(index_file_name, header=False, index=None)
                yield str(split_name)

    def reformat_output(self, raw_result, split_name, exp_params):
        """
        Reformat default output of lcm  to a dataframe with structure : min_support,itemsets,users
        """
        # TODO remove dependency to DatasetHandler
        items = self.dh.get_items()
        output = pd.DataFrame([raw_result[0::2], raw_result[1::2]]).T
        output = pd.concat([output.drop(0, axis=1), output[0].str.split('\(([0-9]+)\)', expand=True).drop(0, axis=1)],
                           axis=1).dropna()
        split_name = split_name.split("/")[-1]  # remove temp folder from name
        output["period"] = split_name.split("_")[0]
        output["property_values"] = "_".join(split_name.split("_")[1:]).split("#")[0]
        output.columns = ["customer_id", "support", "itemset", "period", "property_values"]
        output["itemset"] = output["itemset"].apply(lambda x: str(x).split())
        output["itemset_name"] = output["itemset"].apply(
            lambda x: get_items_descriptions(x, items))
        indexes = pd.read_csv(f'{TMP_FOLDER}/index/{split_name}', header=None)[0].to_dict()
        output["customer_id"] = output["customer_id"].fillna("").map(lambda x: [indexes[int(i)] for i in x.split()])
        output = output[output.support != ""]
        for i in exp_params:
            output[i] = exp_params[i]

        return output

    def run_lcm(self, split_name, itemsets_size, support, exp_params):
        """Runs LCM (Single Thread)  and return the  result formated with format_output

        Example for parameters : input_file='1999',support=6, itemsets_size=[5,100]
        Executed command :  $ ./lcm C_QI -l 5 -u 100 1999 6 -

        Preconfigured parameters:
         C: enumerate closed frequent itemsets
         M: enumerate maximal frequent itemsets
         Q: output the frequency on the head of each itemset found,
         I: output ID's of transactions including each itemset; ID of a transaction is given by the number of line in which the transaction is written. The ID starts from 0.
         _: no output to standard output (including messages w.r.t. input data)
         -l,-u [num]: enumerate itemsets with size at least/most [num]

        Output:
            Replace file having name input_file with the result : support,itemset,users
            if no itemset found the input_file is deleted and output is empty string ""
        """
        support = int(support)
        if None in itemsets_size:
            command = f"""{LCM_EXECUTABLE} C_QI -l {itemsets_size[0]} "{split_name}" {support} -"""
        else:
            command = f"""{LCM_EXECUTABLE} C_QI -l {itemsets_size[0]} -u {itemsets_size[1]} "{split_name}" {support} -"""
        try:
            result = subprocess.check_output(command    , shell=True).decode(sys.stdout.encoding).split('\n')
        except subprocess.CalledProcessError:
            print("No itemset", split_name)
            return
            # os.remove(split_name)
        if "there is no frequent item" in str(result) or result == []:
            print("No itemset", split_name)
            return
        df = self.reformat_output(result, split_name, exp_params)
        df.to_sql(GROUPS_TABLE, if_exists="append", index=None, con=self.engine)
        return split_name

    def linear_closed_itemset_miner(self, df, frequency, min_support, itemsets_size, properties):
        output_file = self.format_output_name(frequency, min_support, itemsets_size, properties)
        try:
            os.remove(output_file)  # In case already existing
            print(f"Removed old {output_file}")
        except:
            print(f"No old file to remove for {output_file} ")

        a = self.multithread_lcm(df, frequency, min_support, itemsets_size, properties, output_file)
        total = len(a._items)
        print(a, total)
        a = [i for i in a if i is not None]
        print(f"---| {output_file} Done")
        print(f'---| #split total: {total}')
        print(f'---| #split having groups: {len(a)}')
        print(f'---| Average: {len(a) / total}')
        print(" ")

    def run(self, df, frequency, support, itemsets_size, properties, exp_params, overwrite=False):
        exp_id = exp_params["sankey_experiment_id"]

        if overwrite:
            self.engine.execute(f""" Delete from "{GROUPS_TABLE}" where sankey_experiment_id='{exp_id}' """)
            self.engine.execute(f""" Delete from "{SANKEY_EXPERIMENT_TABLE}" where sankey_experiment_id='{exp_id}' """)
            self.engine.execute(f""" Delete from "{LINKS_TABLE}" where sankey_experiment_id='{exp_id}' """)

        print("Saving experiment to DB", exp_id)
        pd.DataFrame([exp_params]).to_sql(SANKEY_EXPERIMENT_TABLE, if_exists="append", index=False, con=self.engine)
        data_generator = self.dataset_property_split(df, frequency, properties, support)
        exp_params = {"sankey_experiment_id": exp_params["sankey_experiment_id"]}
        for i in data_generator:
            self.run_lcm(i, itemsets_size=itemsets_size, support=support, exp_params=exp_params)
