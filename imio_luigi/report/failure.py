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


def get_report_config():
    with open("./report.json", "r") as f:
        config = json.load(f)
    return config


def get_result_count(failure_direname):
    namespace = failure_direname.split("-")[0]
    default_prefix = get_report_config()["default_prefix"]
    special_mapping = get_report_config()["special_mapping"]
    if namespace in special_mapping:
        dir_path = f"./{special_mapping[namespace]}"
    else:
        dir_path = f"./{default_prefix}-{namespace}"

    if not os.path.isdir(dir_path):
        return None

    count = len(os.listdir(dir_path))

    return count


def add_color(output, percentage):
    color_mapping = {
        25: "green",
        50: "yellow",
        75: "red",
    }
    color_style = "bright_green"
    for percent, color in color_mapping.items():
        if percentage < percent:
            break
        if percentage >= percent:
            color_style = color

    return click.style(output, fg=color_style)


@cli.command()
@click.option("--percent", default=False, is_flag=True, help="Add percentage of failure against the result")
def list(percent):
    """List tasks in error"""
    for dirname in sorted(os.listdir("./failures")):
        count = len(os.listdir(f"./failures/{dirname}"))
        output = f"{dirname} ({count})"
        if percent:
            result_count = get_result_count(dirname)
            if result_count:
                percentage = (count / result_count) * 100
                output += f" {round(percentage, 2)}%)"
                output = add_color(output, percentage)
            else:
                output += f" (N/A)"
        click.echo(output)


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