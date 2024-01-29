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


def print_list_files(dirnames, pretty, path):
    output = []
    for dirname in dirnames:
        file_list = os.listdir(dirname)
        if path or len(dirnames) > 1:
            file_list = [os.path.join(dirname, filename) for filename in file_list]
        output += file_list

    join_string = " "
    if pretty:
        join_string = "\n"

    click.echo(join_string.join(output))


@cli.command()
def list():
    """List tasks in error"""
    for dirname in sorted(os.listdir("./failures")):
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
      
@cli.command()
@click.argument("task-name")  
@click.option("--pretty", default=False, is_flag=True, help="Pretty output")
@click.option("--path", default=False, is_flag=True, help="Include path in print if only one dirname, if task-name set to 'all' path will be include")
def list_files(task_name, pretty, path):
    """List file in a specific task failure folder"""
    if task_name == "all":
        dirnames = [f"./failures/{f}" for f in os.listdir("./failures")]
    else:
        dirnames = [f"./failures/{task_name}"]
    print_list_files(dirnames, pretty, path)

def main():
    cli()