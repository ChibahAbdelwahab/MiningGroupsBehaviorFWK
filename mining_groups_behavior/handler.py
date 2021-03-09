from mining_groups_behavior import settings
from mining_groups_behavior.dataset_handler import DatasetHandler
from mining_groups_behavior.mining_handler import LcmHandler
from mining_groups_behavior.sankey_generator import SankeyGenerator


def run_mining(dataset, frequency, support, properties, itemsets_size, overwrite=False):
    dh = DatasetHandler(dataset=dataset)
    lh = LcmHandler(dh)
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
    lh.run(df, frequency, support, itemsets_size, properties, exp_params, overwrite=overwrite)
    print("Sankey creation ")
    sg = SankeyGenerator()
    sg.sankey_preprocessing(properties, exp_name=exp_params["sankey_experiment_id"],
                            user_apparition_threshold=1,
                            keep_all_groups_in_periods=[])
    return exp_params["sankey_experiment_id"]


def experiment_name(exp_params):
    name = "_".join(f"{j}" for i, j in exp_params.items())
    name = name.replace("'", "").replace('"', "")
    return name
