# -*- coding: utf-8 -*-
from difflib import SequenceMatcher
from imio_luigi import utils
from statistics import mean


def find_address_similarity(results, term=None):
    normalized_results = [
        {**item, "name": item["name"].split("(")[0].strip()}
        for item in results
    ]
    
    result, error = utils.find_result_similarity(normalized_results, term, key="name")
    
    if result:
        original = next(item for item in results if item["uid"] == result["uid"])
        result = original
    
    if isinstance(error, float):
        error = f"Attention, la ressemblance entre les noms de rue n'est de que {(error)*100}%"
    return result, error


def find_address_match(values, expected_value):
    return utils.find_match(values, expected_value, key="name")
