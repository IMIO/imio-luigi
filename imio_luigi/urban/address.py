# -*- coding: utf-8 -*-
from difflib import SequenceMatcher
from imio_luigi import utils
from statistics import mean





def find_address_similarity(results, term=None):
    result, error = utils.find_result_similarity(results, term, key="name")
    if isinstance(error, float):
        error = f"Attention, la ressemblance entre les noms de rue n'est de que {(error)*100}%"
    return result, error


def find_address_match(values, expected_value):
    return utils.find_match(values, expected_value, key="name")
