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
import copy

FILE_FREQUENCY = {0: 0.01, 1: 0.09, 2: 0.99}
ACCESS_FREQUENCY = {0: 0.15, 1: 0.35, 2: 0.5}

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
        if stats and 'step' in kwargs:
            stats[int(kwargs['step'])][args[0]] = time_taken
        if psl.verbose:
            print(func.__name__, args, kwargs)
        task_queue.task_done()

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
            self.priority_counter = self.metadata["PSL"]["priority_counter"]
            return

        # This is usually an initial setup for a clean system.
        self.latencies = [90, 30] * int(self.num*0.5)
        if int(self.num/2.0)*2 != self.num:
            # uneven number of datanodes
            self.latencies.append(30)
        # SOME CONSTANTS OF THE SYSTEM.
        # 16 GB storage on each
        self.capacities = [17179869184] * self.num # old value 16777216000
        self.available = [17179869184] * self.num
        self.block_size = 67108864
        self.recompute_effectiveness()
        # priority : 0 is high, 1 is medium, 2 is low.
        self.priority_counter = [ 0.0 ] * 3
    
    def recompute_effectiveness(self):
        self.effective = [0.0] * self.num 
        for node_id in range(self.num):
            self.effectiveness_for_node(node_id)

    def effectiveness_for_node(self, i):
        worst_latency = max(self.latencies)*self.capacities[np.argmax(self.latencies)]/self.block_size
        if self.capacities[i] == self.available[i]:
            self.effective[i] = 2*self.latencies[i]/worst_latency
            return
        avg_latency = max(0.5*(self.capacities[i] - self.available[i])*self.latencies[i]/self.block_size, 
                          self.latencies[i])
        # the greater the avg_latency, the worse for us it is
        self.effective[i] = avg_latency/worst_latency

    def persist_metadata(self):
        self.metadata["PSL"] = {
            'capacities': self.capacities,
            'available': self.available,
            'block_size': self.block_size,
            'latencies': self.latencies,
            'effective': self.effective,
            'priority_counter': self.priority_counter
        }
        with open(self.config_dir + 'metadata.json', 'w') as f:
            json.dump(self.metadata, f)

    def execute_command(self, node, command):
        return run(['ssh', node, command])

    def create_file(self, filename, size=67108864, node_id=None, priority=2, persist=True):
        if (filename in self.metadata) or (
            node_id is not None and self.datanodes[node_id] not in self.datanodes):
            return None

        path = self.config['path'] + filename
        command = "head -c {} </dev/urandom > {}".format(size, self.config['path'] + filename)
        if node_id is None:
            node_id = self.placement_node_id(priority)
            if node_id is None:
                return None

        # Important to update the priority counter! This is for data placement algorithm.
        self.priority_counter[priority] += 1
        node = self.datanodes[node_id]

        self.metadata[filename] = {
            'node_id': node_id,
            'node': self.datanodes[node_id],
            'modified': time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
            'priority': priority,
            'size': size
        }

        if persist:
            self.persist_metadata()

        if filename.count('/') > 1:
            basename = filename[:filename.rfind('/')]
        else:
            basename = ''

        self.execute_command(node, 'mkdir -p "{}"'.format(self.config['path'] + basename))
        return  self.execute_command(node, command)

    def get_effective_node_id(self, sorted_eff, priority=2):
        # Sort based on effectiveness
        n = len(sorted_eff)
        node = sorted_eff[0]
        high_pr_share = self.priority_counter[0]/sum(self.capacities)
        if priority == 0 or 1.0* sum(self.available) > 0.1 * sum(self.capacities):
            # High priority
            return node
        elif priority == 1:
            # Medium priority
            if high_pr_share < FILE_FREQUENCY[0]:
                still_missing = FILE_FREQUENCY[0] - high_pr_share
                node = sorted_eff[max(int(still_missing*n), 1)]
        elif priority == 2:
            # Low priority
            med_pr_share = self.priority_counter[1]/sum(self.capacities)
            if high_pr_share < FILE_FREQUENCY[0] or med_pr_share < FILE_FREQUENCY[1]:
                still_missing = ((FILE_FREQUENCY[0] - high_pr_share)
                                 + (FILE_FREQUENCY[1]  - med_pr_share))
                node = sorted_eff[max(int(still_missing*n), 1)]
        return node

    def fake_placement(self, avail, eff, previous_node_id, priority=2):
        sorted_eff = [i[0] for i in sorted(
            enumerate(eff), key=lambda x:x[1], reverse=True)]
        node = self.get_effective_node_id(sorted_eff, priority)
        # No need to move if effectiveness is small.
        if abs(node - previous_node_id) <= 1:
            node = previous_node_id
        # Main formula for effectiveness.
        avail[node] -= self.block_size
        # No more available storage!
        assert avail[node] > 0
        eff[node] = (
            (available[node]/self.capacities[node])**2)/self.latencies[node]
        return node

    def placement_node_id(self, priority=2, persist=True):
        if self.num == 1:
            return 0
        sorted_eff = [i[0] for i in sorted(
            enumerate(self.effective), key=lambda x:x[1], reverse=True)]
        node = self.get_effective_node_id(sorted_eff, priority)

        # print ("Placement_node_id for priority", priority, "is", node)
        # Main formula for effectiveness.
        self.available[node] -= self.block_size
        # No more available storage!
        assert self.available[node] > 0
        self.effectiveness_for_node(node)
        assert self.effective[node] < 1.0
        if persist:
            self.persist_metadata()
        return node


    def placement_reassign(self):
        # Not enough files to consider moving.
        print("Checking placement optimization.")
        if 1.0* sum(self.available) > 0.7 * sum(self.capacities):
            return
        eff = []
        for i in range(self.num):
            eff.append(1.0/self.latencies[i])
        avail = [17179869184] * self.num
        files = sorted(self.metadata.items(), 
            key=lambda k, v: v["priority"] if k != "PSL" else 0.0, reverse=True)
        move = {}
        for filename, value in files:
            if filename == "PSL":
                continue
            best_place = self.fake_placement(avail, eff, value["node_id"], value["priority"]) 
            if best_place != value["node_id"]:
                # we need to move this 
                move[filename] = best_place
        print("Placement reassign: Need to move %d" % len(move))
        if (len(move)) < 1:
            return
        task_list = []
        for filename, node_id in move:
            self.move_file(task_list, filename, node_id, persist)

        psl.submit_tasks(task_list, block=True)
        psl.persist_metadata()

    def move_file(self, task_list, filename, node_id, persist=True):
        if node is None or node not in self.datanodes:
            return
        info = self.metadata[filename]
        task_list.append((psl.delete_file, [filename], {'persist': False}))
        task_list.append((psl.create_file, [filename], {'size': info['size'], 'persist': False, 'priority': info['priority']}))

    def delete_file(self, filename, persist=True):
        if filename not in self.metadata or filename == "PSL":
            return None

        command = "rm -rf {}".format(self.config['path'] + filename)
        node = self.metadata[filename]['node']
        # Recompute statistics
        node_id = self.metadata[filename]['node_id']
        self.available[node_id] += self.block_size
        self.effectiveness_for_node(node_id)
        del self.metadata[filename]
        if persist:
            self.persist_metadata()
        return self.execute_command(node, command)

    def retrieve_file(self, filename, output='./', step=None):
        if filename not in self.metadata or filename == "PSL":
            return None
        else:
            node = self.metadata[filename]['node']
            self.metadata[filename]['modified'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        return run(['scp', node + ':' + self.config['path'] + filename, output])

    def list_files(self):
        if "PSL" not in self.metadata:
            return self.metadata
        tmp = copy.deepcopy(self.metadata)
        del tmp["PSL"]
        return tmp

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
