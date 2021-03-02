import os

import pytest

from core.data_api.dataset_handler import DatasetHandler
from core.mining.LcmHandler import LcmHandler
from tools.lcm_tools import read_lcm_output


# @pytest.mark.parametrize("frequency, min_support, properties,itemsets_size",
#                          [("M", 12, ["sex"], [1, 2]), ("3M", 2, ["sex", "age"], [1, 2])])
# def test_run(frequency, min_support, properties, itemsets_size):
#     lh = LcmHandler()
#     df = DatasetHandler().get_data()
#     lh.linear_closed_itemset_miner(df, frequency, min_support, itemsets_size, properties)
#
#
# @pytest.mark.parametrize("frequency, min_support, properties", [("M", 12, ["sex"]), ("3M", 2, ["sex", "age"])])
# def test_property_split(frequency, min_support, properties):
#     # TODO add check for file content
#     dh = DatasetHandler()
#     lh = LcmHandler()
#     df = dh.get_data()
#     splits = list(lh.dataset_property_split(df, frequency, properties, min_support))
#     pandas.DataFrame([splits]).T.to_csv("splits.csv")
#     assert all(os.path.isfile(i) for i in splits)
#     assert all(os.path.isfile(i.replace('splits', "index")) for i in splits)
#
#
# @pytest.mark.parametrize("frequency, min_support, properties,itemsets_size",
#                          [("M", 2, ["sex"], [1, 2]),
#                           ])
# def test_run_lcm(frequency, min_support, properties, itemsets_size):
#     # TODO mock DatasetHandler
#     dh = DatasetHandler()
#     lh = LcmHandler()
#     df = dh.get_data()
#     splits = list(lh.dataset_property_split(df, frequency, properties, min_support))
#     output_file = lh.format_output_name(frequency, min_support, itemsets_size, properties)
#     lh.run_lcm(splits[1], itemsets_size, min_support, output_file)
#     assert os.path.isfile(output_file)


@pytest.mark.parametrize("frequency, min_support, properties,itemsets_size",
                         [("3M", 2, ["sex"], [2, 100]),
                          ])
def test_run_multi_thread_lcm(frequency, min_support, properties, itemsets_size):
    dh = DatasetHandler()
    lh = LcmHandler()
    df = dh.get_data()
    output_file = lh.format_output_name(frequency, min_support, itemsets_size, properties)
    lh.multithread_lcm(df, frequency, min_support, itemsets_size, properties, output_file)
    assert os.path.isfile(output_file)
    print(output_file)
    read_lcm_output(output_file.split("/")[-1])


if __name__ == '__main__':
    pytest.main(["-vv", __file__])
