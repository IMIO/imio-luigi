# -*- coding: utf-8 -*-

import click
import json
import os


@click.group()
def cli():
    pass


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
    dirname = f"./failures/{task_name}"
    for filename in os.listdir(dirname):
        fpath = os.path.join(dirname, filename)
        with open(fpath, "r") as f:
            data = json.load(f)
            if "reference" in data:
                click.echo(f"{data['reference']}: {data['error']}")
            else:
                click.echo(f"{data['error']}")


def main():
    cli()