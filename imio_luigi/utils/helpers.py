# -*- coding: utf-8 -*-

import os

def get_value_from_path(data, path):
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
