# -*- coding: utf-8 -*-

import click
import json
import os


@click.group()
def cli():
    pass


@cli.command()
@click.argument("filepath")
@click.argument("column")
@click.option("--filter", default="")
@click.option("--example", is_flag=True)
def distinct(filepath, column, filter, example):
    result = {}
    with open(filepath, "r") as f:
        for line in f.readlines():
            data = json.loads(line)
            if column not in data or not data[column]:
                continue
            if filter and data[column] != filter:
                continue
            if data[column] not in result:
                result[data[column]] = {"count": 0, "examples": []}
            result[data[column]]["count"] += 1
            if len(result[data[column]]["examples"]) < 3:
                result[data[column]]["examples"].append(json.dumps(data, indent=2))
    for key, value in result.items():
        click.echo(f"{key}: {value['count']}")
        if example is True:
            for data in value["examples"]:
                click.echo(data)

def clean_str(string):
    if isinstance(string, str) and len(string) > 200:
        return string[:200]
    return string

def clean_dict(dict):
    output = {}
    for key, value in dict.items():
        output[key] = clean_str(value)
    return output

@cli.command()
@click.argument("filepath")
@click.option("--example", is_flag=True)
def keys(filepath, example=False):
    result = {}
    with open(filepath, "r") as f:
        for line in f.readlines():
            data = json.loads(line)
            for key, value in data.items():
                if key not in result:
                    result[key] = value
    if example:
        for element, value in result.items():
            value = clean_str(value)
            if isinstance(value, dict):
                value = clean_dict(value)
            click.echo(f"{element}: {value}")
    else:
        click.echo(", ".join(list(set(result.keys()))))


def main():
    cli()