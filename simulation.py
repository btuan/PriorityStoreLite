""" simulation.py

Simulates a workload on a PriorityStoreLite cluster.

Outputs node statistics as they are available.

Author: Brian Tuan
"""

import click
import json
import time

from api import PriorityStoreLite
from random import random, randrange

""" ACCESS DISTRIBUTION WITH RESPECT TO FILE PRIORITY 
PRIORITY LEVELS:
    0 - HIGH (1% of files)
    1 - MED (9% of files)
    2 - LOW (99% of files)

"""
FILE_FREQUENCY = {0: 0.01, 1: 0.09, 2: 0.99}
ACCESS_FREQUENCY = {0: 0.15, 1: 35, 2: 0.5}


def load_configs(config_dir):
    config_dir = config_dir + '/' if config_dir[-1] != '/' else config_dir
    with open(config_dir + 'simulation.json', 'r') as f:
        config = json.load(f)
    return config


def draw_access_sample(psl, num_files):
    files_hi = [k for k, v in psl.metadata.items() if v['priority'] == 0]
    files_med = [k for k, v in psl.metadata.items() if v['priority'] == 1]
    files_lo = [k for k, v in psl.metadata.items() if v['priority'] == 2]
    files = files_hi + files_med + files_lo

    file_list = []
    for _ in range(num_files):
        num = random()
        if num < ACCESS_FREQUENCY[0] and len(files_hi) > 0:
            file_list.append(files_hi[randrange(len(files_hi))])
        elif num < ACCESS_FREQUENCY[0] + ACCESS_FREQUENCY[1] and len(files_med) > 0:
            file_list.append(files_med[randrange(len(files_med))])
        elif len(files_lo) > 0:
            file_list.append(files_lo[randrange(len(files_lo))])
        else:
            file_list.append(files[randrange(len(files))])

    return file_list


def simulate(config_dir, output_path, verbose):
    config = load_configs(config_dir)
    psl = PriorityStoreLite(config_dir, verbose=verbose)

    # Initialize filesystem
    # First, delete all files that are currently in this PSL instance (synchronous).
    task_list = []
    if verbose:
        print("PSL simulation beginning. Deleting all files currently in system.")
    files = psl.list_files()
    for filename in files.keys():
        task_list.append((psl.delete_file, [filename], {'persist': False}))
    psl.submit_tasks(task_list, block=True)
    psl.persist_metadata()
    print()

    # Next, populate PSL with simulation files (synchronous).
    task_list = []
    if verbose:
        print("Creating PSL simulation files.")
    file_size = config['size_per_file'] if 'size_per_file' in config else None
    for i in range(config['num_files']):
        num = random()
        if num < FILE_FREQUENCY[0]:
            priority = 0
        elif num < FILE_FREQUENCY[0] + FILE_FREQUENCY[1]:
            priority = 1
        else:
            priority = 2

        filename = '{}.psl'.format(i)
        if file_size is not None:
            task_list.append((psl.create_file, [filename], {'size': file_size, 'persist': False, 'priority': priority}))
        else:
            task_list.append((psl.create_file, [filename], {'persist': False, 'priority': priority}))
    psl.submit_tasks(task_list, block=True)
    psl.persist_metadata()
    print()

    # Access a file per second
    print("Simulating file accesses.")
    for i in range(config['duration']):
        file_list = draw_access_sample(psl, config['accesses_per_second'])
        task_list = [(psl.retrieve_file, [name], {'output': '/dev/null'}) for name in file_list]
        psl.submit_tasks(task_list)
        time.sleep(1)


@click.command()
@click.option("-d", "--config_dir", help="Path to directory containing configuration files.", required=True)
@click.option("-o", "--output_path", help="Path to location of simulation output.", default="sim_output.json")
@click.option("-v", "--verbose", default=False, is_flag=True, help="Toggle for verbosity.")
def run(config_dir, output_path, verbose):
    simulate(config_dir, output_path, verbose)


if __name__ == '__main__':
    run()





