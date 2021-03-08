from mining_groups_behavior import settings
from mining_groups_behavior.dataset_handler import DatasetHandler
from mining_groups_behavior.mining_handler import LcmHandler
from tests.mining_test import experiment_name


def run_mining(dataset, frequency, support, properties, itemsets_size):
    dh = DatasetHandler(dataset=dataset)
    lh = LcmHandler()
    df = dh.get_data()
    df = df.rename(columns={"article_id": "item_id"})
    exp_params = {
        "dataset": "Retail",
        "time_granularity": frequency,
        "support": support,
        "properties": str(properties),
        "itemsets_size": str(itemsets_size),
    }
    exp_params["sankey_experiment_id"] = experiment_name(exp_params)
    exp_params["sankey_experiment_id"].replace("'", '')
    lh.run(df, frequency, support, itemsets_size, properties, exp_params, overwrite=False)

