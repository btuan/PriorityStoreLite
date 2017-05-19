#!/usr/bin/env python3

""" cli.py

Command-line interface for PriorityStoreLite. Requires click
    $ pip3 install click

Author: Brian Tuan

"""

import click
from api import PriorityStoreLite


@click.command()
@click.option("-d", "--config_dir", help="Path to directory containing configuration files.", required=True)
@click.option("-c", "--command", help="PriorityStore API command.")
@click.option("-v", "--verbose", default=False, is_flag=True, help="Toggle for verbosity.")
def run(config_dir, command, verbose):
    psl = PriorityStoreLite(config_dir)

    return psl


if __name__ == '__main__':
    run()