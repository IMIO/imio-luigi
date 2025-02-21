# -*- coding: utf-8 -*-

from statistics import fmean, median

import click
import json
import os
import re


@click.group()
def cli():
    pass


def get_value(data, path):
    path_split = path.split("/")
    current_data = data
    for count, key in enumerate(path_split):
        if isinstance(current_data, dict) and key in current_data:
            current_data = current_data[key]
        elif isinstance(current_data, list):
            output = [get_value(item_data, key) for item_data in current_data]
            return output
        else:
            return None
    return current_data


def match_value(value, check):
    match = re.match(check, value)
    return bool(match)


def check_value(value, check=None):
    if isinstance(value, list):
        if check:
            return [1 for item in value if match_value(item, check)]
        else:
            return [
                1 if item is not None and item != "" else 0 for item in value
            ]
    if check:
        return [1 if match_value(value, check) else 0]
    else:
        return [1]


def get_title_config(config):
    return config.get("id", config["key"])


def get_stat_data(path, configs, select=None):
    files = os.listdir(path)
    output = {
        get_title_config(config): {
            "results": {},
            "type": config.get("type", "normal")
        }
        for config in configs
    }
    for filename in files:
        fpath = os.path.join(path, filename)
        with open(fpath, 'r') as f:
            data = json.load(f)
        for config in configs:
            if select is not None and get_title_config(config) not in select:
                continue
            key = config["key"]
            data_value = get_value(data, key)
            if not data_value:
                continue
            value_to_chek = config.get("value", None)
            result = sum(check_value(data_value, value_to_chek))
            output[get_title_config(config)]["results"][data["reference"]] = {
                "path": filename,
                "result": result
            }
    return output, files


def unique_value_count(list):
    output = {}
    for item in list:
        if item in output:
            output[item] = output[item] + 1
        else:
            output[item] = 1
    return output


def print_unique_values(values, total):
    click.echo(f"\t\tUnique value stats :")
    all_values_sum = sum(values.values())
    remain_values = total - all_values_sum
    if 0 in values:
        values[0] = values[0] + remain_values
    else:
        values[0] = remain_values
    for key, value in values.items():
        percentage = (value / total) * 100
        click.echo(f"\t\t\t{key}: {value} (Percentage: {percentage:.2f}%)")


@cli.command()
@click.argument("path")
@click.argument("config")
def stat(path, config):
    """Display stat of a series of key for all result"""
    with open(config, "r") as f:
        configs = json.load(f)
    output, files = get_stat_data(path, configs)

    click.echo(f"Stat (for {len(files)} files):")

    for out in output:
        title = out
        click.echo(f"\t{title}:")
        total_count = len(files)
        values = output[out]["results"]
        result_type = output[out]["type"]
        list_values = [value["result"] for value in values.values() if value["result"] >= 1]
        count = len(list_values)
        click.echo(f"\t\tCount: {count}")
        percentage = (count / total_count) * 100
        click.echo(f"\t\tPercentage: {percentage:.2f}%")
        if len(list_values) < 1:
            continue
        min_value = min(list_values)
        max_value = max(list_values)
        click.echo(f"\t\tMin/Max: {min_value} / {max_value}")
        median_value = median(list_values)
        mean_value = fmean(list_values)
        click.echo(f"\t\tMedian/Average: {median_value:.2f} / {mean_value:.2f}")
        if result_type == "list":
            value_by_count = unique_value_count(list_values)
            print_unique_values(value_by_count, total_count)


@cli.command()
@click.argument("path")
@click.argument("config")
@click.argument("filter-path")
def filter(path, config, filter_path):
    with open(config, "r") as f:
        configs = json.load(f)
    with open(filter_path, "r") as f:
        filter = json.load(f)

    stats, files = get_stat_data(path, configs, select=filter["key"])
    results = stats[filter["key"]]
    output = []
    for result in results:
        obj = results[result]["results"]
        value = obj["result"]
        filename = obj["path"]
        eval_string = f"{value}{filter['operator']}{filter['value']}"
        if eval(eval_string):
            output.append(filename)

    click.echo(" ".join(output))


@cli.command()
@click.argument("key")
@click.argument("path")
@click.option("--display-no-key", default=False, is_flag=True, help="Display file where the key wasn't found")
@click.option("--unique", default=False, is_flag=True, help="Only show unique key")
@click.option("--count", default=False, is_flag=True, help="Show count occurence when unique selected")
def list_key(key, path, display_no_key, unique, count):
    """List value of a specifc key in json result"""
    files = os.listdir(path)
    output = []
    no_key = []
    count_dict = {}
    for filename in files:
        fpath = os.path.join(path, filename)
        with open(fpath, 'r') as f:
            data = json.load(f)
        if key in data:
            output.append(data[key])
            if count:
                if data[key] not in count_dict:
                    count_dict[data[key]] = 0
                count_dict[data[key]] += 1
        else:
            no_key.append(filename)

    if unique:
        output = list(set(output))

    if count:
        output = [f"{item}: {int(count_dict.get(item, 0))}" for item in output]

    if len(output) > 0:
        click.echo(f"List of value for the key '{key}' found in '{path}'")
        click.echo("\n".join(sorted(output)))
    else:
        click.echo(f"The key ({key}) not found in files in '{path}'")

    if display_no_key:
        click.echo(f"These files doesn't have the key '{key}'")
        click.echo("\n".join(sorted(no_key)))


def main():
    cli()
