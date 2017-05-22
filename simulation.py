""" simulation.py

Simulates a workload on a PriorityStoreLite cluster.

Outputs node statistics as they are available.

Author: Brian Tuan
"""

import click
import json
import time

from multiprocessing import Queue, Pool, cpu_count
from api import PriorityStoreLite


def load_configs(config_dir):
    config_dir = config_dir + '/' if config_dir[-1] != '/' else config_dir
    with open(config_dir + 'simulation.json', 'r') as f:
        config = json.load(f)
    return config


def simulate(config_dir, output_path, verbose):
    config = load_configs(config_dir)
    psl = PriorityStoreLite(config_dir, verbose=verbose)
    pool = Pool(cpu_count() * 2)

    # Initialize filesystem
    # First, delete all files that are currently in this PSL instance (synchronous).
    task_list = []
    if verbose:
        print("PSL simulation beginning. Deleting all files currently in system.")
    files = psl.list_files()
    for filename in files.keys():
        task_list.append((psl.delete_file, [filename], {'persist': False}))
    psl.submit_tasks(task_list)
    psl.persist_metadata()

    # Next, populate PSL with simulation files (synchronous).
    task_list = []
    if verbose:
        print("Creating PSL simulation files.")
    file_size = config['size_per_file'] if 'size_per_file' in config else None
    for i in range(config['num_files']):
        filename = '{}.psl'.format(i)
        if file_size is not None:
            task_list.append((psl.create_file, [filename], {'size': file_size, 'persist': False}))
        else:
            task_list.append((psl.create_file, [filename], {'persist': False}))
    psl.submit_tasks(task_list)
    psl.persist_metadata()



@click.command()
@click.option("-d", "--config_dir", help="Path to directory containing configuration files.", required=True)
@click.option("-o", "--output_path", help="Path to location of simulation output.", default="sim_output.json")
@click.option("-v", "--verbose", default=False, is_flag=True, help="Toggle for verbosity.")
def run(config_dir, output_path, verbose):
    simulate(config_dir, output_path, verbose)


if __name__ == '__main__':
    run()





