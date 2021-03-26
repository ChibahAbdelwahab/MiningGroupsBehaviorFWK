def label_groups(x):
    #     return random.choice(["1","2","3",4"])
    if len(x["source_users"]) == len(x["intersection"]):
        if len(x["source_users"]) == len(x["target_users"]):
            return "S"  # 'stable'
        return "G"  # "grows"
    if len(x["target_users"]) == len(x["intersection"]):
        return "ST"  # "Split"
    return "M"  # "Merge"
