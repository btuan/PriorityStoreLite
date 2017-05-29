""" simulation.py

Simulates a workload on a PriorityStoreLite cluster.

Outputs node statistics as they are available.

Author: Brian Tuan
"""

import click
import json
import time
import sys

from api import PriorityStoreLite, Logger
from random import random, randrange

""" ACCESS DISTRIBUTION WITH RESPECT TO FILE PRIORITY 
PRIORITY LEVELS:
    0 - HIGH (1% of files)
    1 - MED (9% of files)
    2 - LOW (99% of files)

"""
FILE_FREQUENCY = {0: 0.01, 1: 0.09, 2: 0.99}
ACCESS_FREQUENCY = {0: 0.15, 1: 0.35, 2: 0.5}

# We want to not only print but also save the output. 
sys.stdout = Logger()

def load_configs(config_dir):
    config_dir = config_dir + '/' if config_dir[-1] != '/' else config_dir
    with open(config_dir + 'simulation.json', 'r') as f:
        config = json.load(f)
    return config


def draw_access_sample(psl, num_files):
    files_hi = [k for k, v in psl.metadata.items() if k != "PSL" and v['priority'] == 0]
    files_med = [k for k, v in psl.metadata.items() if k != "PSL" and v['priority'] == 1]
    files_lo = [k for k, v in psl.metadata.items() if k != "PSL" and v['priority'] == 2]
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

def set_bottomline_latency(psl, config, size_per_file=67108864, verbose=True):
    if verbose:
        print("Determining the default latency")

    filename = "latency.psl"
    psl.create_file(filename, size=size_per_file, priority=1, persist=False)
    time_before = time.time()
    psl.retrieve_file(filename)
    time_after = time.time()
    time_taken = time_after - time_before
    psl.default_latency = time_taken
    psl.latency_diff = time_taken * config["latency_difference"]
    print("Setting initial latency differences for {} nodes".format(psl.num))
    psl.latencies = [time_taken*(1+config["latency_difference"]), time_taken] * int(psl.num*0.5)
    if int(psl.num/2.0)*2 != psl.num:
        # uneven number of datanodes
        psl.latencies.append(time_taken)
    psl.delete_file(filename)
    print(psl.latencies)

def simulate(config_dir, output_path, verbose):
    config = load_configs(config_dir)
    psl = PriorityStoreLite(config_dir, verbose=verbose)
    # Determine current latencies of the datanodes
    set_bottomline_latency(psl, config)
     
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
    # Reset PSL metadata
    del psl.metadata["PSL"]
    psl.setup_system_info()
    print()

    # Next, populate PSL with simulation files (synchronous).
    file_size = config['size_per_file'] if 'size_per_file' in config else None
    if verbose:
        print("Creating PSL simulation files, each of size {}.".format(file_size))
    task_list = []
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
    psl.print_stats()
    print()

    # Access a file per second
    stats = {}
    print("Simulating file accesses.")
    for i in range(config['duration']):
        print("Time step", i)
        stats[i] = {}
        file_list = draw_access_sample(psl, config['accesses_per_second'])
        task_list = [(psl.retrieve_file, [name], {'output': '/dev/null', 'step' : i}) for name in file_list]
        psl.submit_tasks(task_list, stats=stats)
        time.sleep(1)
        print()

    time.sleep(10)
    print("Stats", stats)
    with open('stats.json', 'w') as f:
            json.dump(stats, f)

    file_stats = {}
    priority_stats = {}
    for run, value in stats.items():
        for filename, l in value.items():
            latency = l
            if psl.metadata[filename]["node_id"] % 2 != 0:
                # adding default latency difference
                latency += psl.latency_diff
            pr = psl.metadata[filename]['priority']
            if pr not in priority_stats:
                priority_stats[pr] = {}
                priority_stats[pr]["latency"] = 0.0
                priority_stats[pr]["counter"] = 0
            priority_stats[pr]["latency"] += latency
            priority_stats[pr]["counter"] += 1
            # Add statistics on files
            if filename not in file_stats:
                file_stats[filename] = {}
                file_stats[filename]["latency"] = 0.0
                file_stats[filename]["counter"] = 0
            file_stats[filename]["latency"] += latency
            file_stats[filename]["counter"] += 1
    # Save again, now that we've updated the latencies
    with open('stats.json', 'w') as f:
            json.dump(stats, f)

    for filename, value in file_stats.items():
        print(filename, psl.metadata[filename]['priority'], "%0.3f" % (value["latency"]/value["counter"]))
    
    for pr, value in priority_stats.items():
        print("Priority", pr, "%0.3f" % (value["latency"]/value["counter"]), " with # {} files".format(value["counter"]))
    # And finally...
    psl.print_stats()


@click.command()
@click.option("-d", "--config_dir", help="Path to directory containing configuration files.")
@click.option("-o", "--output_path", help="Path to location of simulation output.", default="sim_output.json")
@click.option("-v", "--verbose", default=True, is_flag=True, help="Toggle for verbosity.")
def run(config_dir, output_path, verbose):
    if not config_dir:
        config_dir = "config"
    simulate(config_dir, output_path, verbose)


if __name__ == '__main__':
    run()





