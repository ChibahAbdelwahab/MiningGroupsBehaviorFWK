import os

from mining_groups_behavior.sankey_generator import SankeyGenerator
from mining_groups_behavior.settings import GROUPS_FOLDER


def test_sankey_generator():
    sg = SankeyGenerator()
    input_file = "3M-2-[2-100]-[sex]-lcm.out"
    exp_id = "Retail_M_10_[sex]_[1, None]"
    e = sg.sankey_preprocessing(input_file, exp_name=exp_id, user_apparition_threshold=1, keep_all_groups_in_periods=[])
    assert os.path.isfile(f"{GROUPS_FOLDER}/{input_file}")
    assert os.path.isfile(sg.generate_sankey_file(input_file))


if __name__ == '__main__':
    test_sankey_generator()
    # pytest.main(["-vv", __file__])
