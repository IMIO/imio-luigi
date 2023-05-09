# -*- coding: utf-8 -*-

import click
import json
import os


@click.group()
def cli():
    pass


def print_report(dirname):
    result = {}
    click.echo(f"Failures for {dirname}")
    for filename in os.listdir(dirname):
        fpath = os.path.join(dirname, filename)
        with open(fpath, "r") as f:
            data = json.load(f)
            if "reference" in data:
                msg = f"{data['reference']}: {data['error']}"
            else:
                msg = f"{data['error']}"
            if msg not in result:
                result[msg] = 1
            else:
                result[msg] += 1
    sorted_result = sorted([(v, k) for k, v in result.items()], reverse=True)
    for nbr, msg in sorted_result:
        click.echo(f"{nbr}: {msg}")


@cli.command()
def list():
    """List tasks in error"""
    for dirname in os.listdir("./failures"):
        count = len(os.listdir(f"./failures/{dirname}"))
        click.echo(f"{dirname} ({count})")


@cli.command()
@click.argument("task-name")
def report(task_name):
    """List errors for a given task"""
    if task_name == "all":
        dirnames = [f"./failures/{f}" for f in os.listdir("./failures")]
    else:
        dirnames = [f"./failures/{task_name}"]
    for dirname in dirnames:
        print_report(dirname)


def main():
    cli()