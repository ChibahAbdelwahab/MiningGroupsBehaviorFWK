from sqlalchemy import create_engine

from src.settings.settings import RESULTS_FOLDER, DATABASE_URL


class MiningHandler:
    """ This Class gives an api to use LCM algorithm """

    def __init__(self):
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
