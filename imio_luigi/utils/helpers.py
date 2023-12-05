# -*- coding: utf-8 -*-

def get_value_from_path(data, path):
    path_split = path.split("/")
    current_data = data
    for key in path_split:
        if isinstance(current_data, dict) and key in current_data:
            current_data = current_data[key]
        else:
            return None
    return current_data