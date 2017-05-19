#!/usr/bin/env python3

""" cli.py

Command-line interface for PriorityStoreLite. Requires click
    $ pip3 install click

Author: Brian Tuan
"""

import click
from api import PriorityStoreLite
from pprint import PrettyPrinter


@click.command()
@click.option("-d", "--config_dir", help="Path to directory containing configuration files.", required=True)
@click.option("-c", "--command", help="PriorityStore API command.")
@click.option("-v", "--verbose", default=False, is_flag=True, help="Toggle for verbosity.")
def run(config_dir, command, verbose):
    psl = PriorityStoreLite(config_dir)
    p = PrettyPrinter(indent=4)

    cmd = command.strip().split(' ')
    if cmd[0] == 'ls':
        print("Listing files...")
        p.pprint(psl.list_files())

    elif cmd[0] == 'del':
        if len(cmd) != 2:
            print('ERROR: del command requires two arguments.')
        rc = psl.delete_file(cmd[1])
        if rc is None:
            print('ERROR: {} does not exist in the filesystem.'.format(cmd[1]))

    elif cmd[0] == 'get':
        if len(cmd) == 2:
            rc = psl.retrieve_file(cmd[1])
            if rc is None:
                print('ERROR: {} does not exist in the filesystem.'.format(cmd[1]))
        elif len(cmd) == 3:
            rc = psl.retrieve_file(cmd[1], output=cmd[2])
            if rc is None:
                print('ERROR: {} does not exist in the filesystem.'.format(cmd[1]))

    elif cmd[0] == 'put':
        if len(cmd) == 2:
            rc = psl.create_file(cmd[1])
            if rc is None:
                print('ERROR: {} already exists in the filesystem.'.format(cmd[1]))
        elif len(cmd) == 3:
            rc = psl.create_file(cmd[1], node=cmd[2])
            if rc is None:
                print('ERROR: {} already exists in the filesystem'.format(cmd[1]) + ' OR ' +
                      '{} is not a valid user@FQDN specification.'.format(cmd[2]))
        else:
            print('ERROR: syntax --- ./cli.py put <filename> <node>')

    else:
        print("Command not recognized: " + command)

    return psl


if __name__ == '__main__':
    run()