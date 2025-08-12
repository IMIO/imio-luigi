# -*- coding: utf-8 -*-

import os
from difflib import SequenceMatcher
from statistics import mean


SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITH_WARNING = 0.9
SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITHOUT_WARNING = 1
SIMILARITY_THRESHOLD_WITH_TERM_WITH_WARNING = 0.9
SIMILARITY_THRESHOLD_WITH_TERM_WITHOUT_WARNING = 1

def get_value_from_path(data, path):
    """
    Return value from a dict with a path seprate with /

    :param data: data where to search the value
    :type data: dict
    :param path: path to the data
    :type path: string
    :return: value or values correspondting to the path found in the data
    :rtype: string or list of string
    """
    path_split = path.split("/")
    current_data = data
    for key in path_split:
        if isinstance(current_data, dict) and key in current_data:
            current_data = current_data[key]
        elif isinstance(current_data, list):
            output = [
                get_value_from_path(item_data, key)
                for item_data in current_data
                if get_value_from_path(item_data, key)
            ]
            return output
        else:
            return None
    return current_data


def set_value_from_path(data, path, value):
    keys = path.split("/")
    d = data
    for key in keys[:-1]:
        d = d.setdefault(key, {})  # Create nested dicts if missing
    d[keys[-1]] = value
    return data


def get_value_from_path_with_parents(data, path, parents=None):
    """
    Return value from a dict with a path seprate with /
    If there is multiple possiblity of path to acces some data
    can be added list of potential parent

    :param data: data where to search the value
    :type data: dict
    :param path: path to the data
    :type path: string
    :param parents: list of potential parents to acces value, defaults to None
    :type parents: list of string, optional
    :return: value or values correspondting to the path found in the data
    :rtype: string list of string
    """
    if parents is None:
        return get_value_from_path(path, path)
    for parent in parents:
        value = get_value_from_path(data, os.path.normpath(os.path.join(parent, path)))
        if value is None:
            continue
        return value
    return None


def fix_search_term(term):
    """Fix term with parentheses"""
    term = term.replace("(", " ")
    term = term.replace(")", " ")
    return term.strip()


def add_data_to_description(data, value):
    if "description" not in data:
        data["description"] = {
            "content-type": "text/html",
            "data": "",
        }
    data["description"]["data"] += value
    return data


def calculate_similarity(str1, str2):
    seq_matcher = SequenceMatcher(None, str1, str2)
    return seq_matcher.ratio()


def get_value_conditional(data, key=None):
    if not key:
        return data
    return data[key]


def find_most_similar_result(results, key=None):
    num_results = len(results)

    similarity_list = []

    for i in range(num_results):
        for j in range(i + 1, num_results):
            similarity_list.append(
                calculate_similarity(
                    get_value_conditional(results[i], key),
                    get_value_conditional(results[j], key)
                )
            )

    if all(filter(lambda a: a > SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITHOUT_WARNING, similarity_list)):
        return True, None
    elif all(filter(lambda a: a > SIMILARITY_THRESHOLD_BETWEEN_RESULTS_WITH_WARNING, similarity_list)):
        return True, similarity_list
    else:
        return False, f"Pas assez d'assurance qu'il y a de la ressemblance entre les resultats trouvé."


def find_most_similar_term(term, results, key=None):
    highest_similarity = 0
    most_similar_result = None

    for result in results:
        similarity_ratio = calculate_similarity(term, get_value_conditional(result, key))

        if similarity_ratio > highest_similarity:
            highest_similarity = similarity_ratio
            most_similar_result = result

    return most_similar_result, highest_similarity


def find_match(values, expected_value, key=None):
    exact_match = []
    for item in values:
        name = get_value_conditional(item, key).split("(")[0].strip()
        if name.upper() == expected_value.strip().upper():
            exact_match.append(item)
    if len(exact_match) == 1:
        return exact_match[0]
    return []


def find_result_similarity(results, term=None, key=None):

    similarity_between_results, error = find_most_similar_result(results, key=key)

    if similarity_between_results:
        return results[0], error

    if term:
        most_similar_result, highest_similarity = find_most_similar_term(term, results, key=key)
        if highest_similarity > SIMILARITY_THRESHOLD_WITH_TERM_WITHOUT_WARNING:
            return most_similar_result, None
        elif highest_similarity > SIMILARITY_THRESHOLD_WITH_TERM_WITH_WARNING:
            return most_similar_result, highest_similarity

    return None, None