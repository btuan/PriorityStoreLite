""" api.py

Python API for PriorityStoreLite. Built on top of commonly available UNIX tools.

Exposes a simple CRUD API. Assumes that user to which SSH is done to has public key SSH configured.

Author: Brian Tuan
"""

import json
from multiprocessing import cpu_count
import queue
from random import randrange
from subprocess import run
import time
import numpy as np
from threading import Thread
import sys

def psl_worker(psl, task_queue, stats):
    while True:
        command = task_queue.get()
        if command is None:
            break
        func, args, kwargs = command
        time_before = time.time()
        func(*args, **kwargs)
        time_after = time.time()
        time_taken = time_after - time_before
        # Save the statistics
        if stats:
            stats[int(kwargs['step'])][args[0]] = time_taken
        if psl.verbose:
            print(func.__name__, args, kwargs)
        task_queue.task_done()

# credit to : https://stackoverflow.com/questions/616645/how-do-i-duplicate-sys-stdout-to-a-log-file-in-python
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("log.txt", "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()

class PriorityStoreLite:
    def __init__(self, config_dir, verbose=False):
        self.verbose = verbose
        self.config_dir = config_dir + '/' if config_dir[-1] != '/' else config_dir

        with open(self.config_dir + 'config.json', 'r') as f:
            self.config = json.load(f)
            if self.config is None:
                self.config = {}

        with open(self.config_dir + 'metadata.json', 'r') as f:
            self.metadata = json.load(f)
            if self.metadata is None:
                self.metadata = {}

        with open(self.config_dir + 'datanodes.json', 'r') as f:
            self.datanodes = json.load(f)['nodelist']
            assert(len(self.datanodes) != 0)

        self.setup_system_info()
        # sys.stdout = Logger()

    def setup_system_info(self):
        self.num = len(self.datanodes)

        if "PSL" in self.metadata:
            self.block_size = self.metadata["PSL"]["block_size"]
            self.available = self.metadata["PSL"]["available"]
            self.capacities = self.metadata["PSL"]["capacities"]
            self.latencies = self.metadata["PSL"]["latencies"]
            self.effective = self.metadata["PSL"]["effective"]
            return

        # This is usually an initial setup for a clean system.
        self.latencies = [90, 30] * int(self.num*0.5)
        if int(self.num/2.0)*2 != self.num:
            # uneven number of datanodes
            self.latencies.append(30)
        self.effective = []
        for i in range(self.num):
            self.effective.append(1.0/self.latencies[i])
        # SOME CONSTANTS OF THE SYSTEM.
        # 16 GB storage on each
        self.capacities = [17179869184] * self.num # old value 16777216000
        self.available = [17179869184] * self.num
        self.block_size = 67108864

    def persist_metadata(self):
        self.metadata["PSL"] = {
            'capacities': self.capacities,
            'available': self.available,
            'block_size': self.block_size,
            'latencies': self.latencies,
            'effective': self.effective
        }
        with open(self.config_dir + 'metadata.json', 'w') as f:
            json.dump(self.metadata, f)

    def execute_command(self, node, command):
        return run(['ssh', node, command])

    def create_file(self, filename, size=67108864, node=None, priority=0, persist=True):
        if filename in self.metadata or (node is not None and node not in self.datanodes):
            return None

        path = self.config['path'] + filename
        command = "head -c {} </dev/urandom > {}".format(size, self.config['path'] + filename)
        node_id = self.placement_node_ids()
        if node_id is None:
            return None
        node = self.datanodes[node_id]

        self.metadata[filename] = {
            'node_id': node_id,
            'node': self.datanodes[node_id],
            'modified': time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
            'priority': priority
        }
        if persist:
            self.persist_metadata()

        if filename.count('/') > 1:
            basename = filename[:filename.rfind('/')]
        else:
            basename = ''

        self.execute_command(node, 'mkdir -p "{}"'.format(self.config['path'] + basename))
        return  self.execute_command(node, command)
        # print("File creation has failed!")
        # return 1.0

    def placement_node_ids(self):
        node = np.argmax(self.effective)
        # Main formula for effectiveness.
        self.available[node] -= self.block_size
        # No more available storage!
        if (self.available[node] < 0):
            # Roll back the change
            self.available[node] += self.block_size
            return None
        # update the effectiveness of the node.
        self.effective[node] = (
            (self.available[node]/self.capacities[node])**2)/self.latencies[node]
        self.persist_metadata()
        print ("Choosing node ", node, " because ", self.effective, self.available)
        return np.asscalar(node)

    def delete_file(self, filename, persist=True):
        if filename not in self.metadata or filename == "PSL":
            return None

        command = "rm -rf {}".format(self.config['path'] + filename)
        node = self.metadata[filename]['node']
        del self.metadata[filename]

        if persist:
            self.persist_metadata()
        return self.execute_command(command, node)

    def retrieve_file(self, filename, output='./', step=None):
        if filename not in self.metadata or filename == "PSL":
            return None
        else:
            node = self.metadata[filename]['node']
            self.metadata[filename]['modified'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        return run(['scp', node + ':' + self.config['path'] + filename, output])

    def list_files(self):
        return self.metadata

    def submit_tasks(self, task_list, block=False, stats=None):
        """ Asynchronously process tasks in a task list, given as (func, args, kwargs). """
        num_workers = cpu_count()
        threads = []
        task_queue = queue.Queue()
        for _ in range(num_workers):
            t = Thread(target=psl_worker, args=[self, task_queue, stats])
            t.start()
            threads.append(t)

        for task in task_list:
            task_queue.put(task)

        # https://docs.python.org/3/library/queue.html
        for _ in range(num_workers):
            task_queue.put(None)

        if block:
            for t in threads:
                t.join()

    def print_file_info(self):
        print("Filename, Priority, Node latency, Node")
        print ("________________________________________")
        for filename, info in self.metadata.items():
            if filename == "PSL":
                continue
            node_id = info["node_id"]
            print(filename, info["priority"], self.latencies[node_id], info["node"])
        print()

    def print_node_info(self):
        print ("Node id,  available, latency, files")
        print ("____________________________")
        for i in range(self.num):
            files = []
            for filename, info in self.metadata.items():
                if filename == "PSL":
                    continue
                node_id = info["node_id"]
                if node_id == i:
                    files.append(filename)
            print(i, "        %0.3f" % (self.available[i]/1073741824.0), "GB   ", 
                  self.latencies[i], "  ", files)
        print()

    def print_stats(self):
        print()
        self.print_node_info()
        self.print_file_info()

# End of file
