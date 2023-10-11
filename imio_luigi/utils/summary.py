# -*- coding: utf-8 -*-

def get_all_keys(items):
    key_list = set()
    counter = 0
    for item in items:
        keys = item.keys()
        temp_list = list(key_list) + list(keys)
        key_list = set(temp_list)
        counter += 1

    return key_list

def get_all_unique_value(data, column):
    return (item[column] for item in data if column in item)
    