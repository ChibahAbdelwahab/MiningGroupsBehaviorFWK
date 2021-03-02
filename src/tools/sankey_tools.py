def label_groups(x):
    #     return random.choice(["1","2","3",4"])
    print(len(x["source_users"]) - len(x["intersection"]), 0.50 * len(x["source_users"]))
    if len(x["source_users"]) - len(x["intersection"]) <= 0.5 * len(x["source_users"]):
        if len(x["source_users"]) == len(x["target_users"]):
            return "S"  # 'stable'
        return "G"  # "grows"
    if len(x["target_users"]) == len(x["intersection"]):
        return "Su"  # "subset"
    return "SG"  # "subset_grows"
