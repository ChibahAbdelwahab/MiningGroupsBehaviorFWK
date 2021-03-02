def get_items_descriptions(x, items):
    """
    Returns a list of items description from a list of items ids
    """
    x = x.replace(".0", "")
    if isinstance(x, int) or isinstance(x, float):
        x = str(x).replace(".0", " ")
    return items.loc[[int(i) for i in x.split()]].description.tolist()
