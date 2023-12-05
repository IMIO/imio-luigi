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
    return set(item[column] for item in data if column in item)

def get_all_unique_value_with_callback(data, callback):
    return set(callback(item) for item in data)

def get_all_unique_values_with_first_ref(data, column, ref):
    output = {}
    for item in data:
        if item[column] in output:
            continue
        output[item[column]] = item[ref]
        
    return output
