import re

import numpy as np


def age_class(age):
    """User age to range classes """
    age = np.int(age)
    if age <= 25:
        return 0
    if age <= 45:
        return 1
    return 2


def extract_demographics(input_file):
    demographics = re.findall("\[([A-Z|a-z|_]+),?([A-Z|a-z|_]+)?\]", input_file)[0]
    return [i for i in demographics if i != ""]
