# -*- coding: utf-8 -*-


def find_address_match(values, expected_value):
    exact_match = []
    for item in values:
        name = item["name"].split("(")[0].strip()
        if name.upper() == expected_value.strip().upper():
            exact_match.append(item)
    if len(exact_match) == 1:
        return exact_match[0]
    return []