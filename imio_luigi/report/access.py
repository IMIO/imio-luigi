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


def main():
    cli()