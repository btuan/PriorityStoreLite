""" api.py

Python API for PriorityStoreLite. Built on top of commonly available UNIX tools.

Exposes a simple CRUD API. Assumes that user to which SSH is done to has public key SSH configured.

Author: Brian Tuan
"""

import json
from random import randrange
from subprocess import run
import time
import numpy as np
from multiprocessing import Process


class PriorityStoreLite:
    def __init__(self, config_dir):
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

        self.num = len(self.datanodes)
        self.latencies = [90, 30] * int(self.num*0.5)
        if int(self.num/2.0)*2 != self.num:
            # uneven number of datanodes
            self.latencies.append(30)
        self.effective = []
        for i in range(self.num):
            self.effective = [1.0/self.latencies[i]]
        # SOME CONSTANTS OF THE SYSTEM.
        # 16 GB storage on each
        self.capacities = [16777216000] * self.num
        self.available = [16777216000] * self.num
        self.block_size = 67108864

    def persist_metadata(self):
        self.metadata["PSL"] = {
            'capacities': self.capacities,
            'available': self.available,
            'block_size': self.block_size,
            'latencies': self.latencies,
        }
        with open(self.config_dir + 'metadata.json', 'w') as f:
            json.dump(self.metadata, f)

    def execute_command(self, node, command):
        return run(['ssh', node, command])

    def execute_scp(self, node, filename, output='./'):
        return run(['scp', node + ':' + self.config['path'] + filename, output])

    def execute_create(self, node, basename, command):
        self.execute_command(node, 'mkdir -p "{}"'.format(self.config['path'] + basename))
        self.execute_command(node, command)

    def execute_on_nodes(self, nodes, func, args):
        ps = []
        # spawn threads
        for i in nodes:
            node = self.datanodes[i]; tmp_args = (node,) + args
            p = Process(target=func, args=tmp_args)
            p.start(); ps.append(p)
        # join all the threads
        for i in nodes:
            ps[i].join()

    def create_file(self, filename, size=67108864*2, node=None, priority=0):
        if filename in self.metadata:
            return None
        
        if node is None:
            node = self.datanodes[randrange(len(self.datanodes))]
        elif node not in self.datanodes:
            return None

        path = self.config['path'] + filename
        num_blocks = int(np.ceil(size*1.0/self.block_size))
        command = "head -c {} </dev/urandom > {}".format(self.block_size, self.config['path'] + filename)
        nodes = self.placement_node_ids(num_blocks)
        if nodes is None:
            return None

        self.metadata[filename] = {
            'nodes': nodes,
            'modified': time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
            'priority': priority,
        }
        self.persist_metadata()

        if filename.count('/') > 1:
            basename = filename[:filename.rfind('/')]
        else:
            basename = ''

        self.execute_on_nodes(nodes, self.execute_create, args=(basename, command))
        return 1.0

    def placement_node_ids(self, num_blocks=3):
        nodes = []
        for i in range(num_blocks):
            node = np.argmax(self.effective)
            # Main formula for effectiveness
            self.available[node] -= self.block_size
            # No more available storage!
            if (self.available[node] < 0):
                # Roll back the change
                self.available[node] += self.block_size
                return None
            nodes.append(np.asscalar(node))
            self.effective[node] = ((self.available[node]/self.capacities[node])**2)/self.latencies[node]

        return nodes

    def delete_file(self, filename):
        if filename not in self.metadata:
            return None

        command = "rm -rf {}".format(self.config['path'] + filename)
        nodes = self.metadata[filename]['nodes']
        del self.metadata[filename]

        self.persist_metadata()
        self.execute_on_nodes(nodes, self.execute_command, args=(command,))
        return 1.0

    def retrieve_file(self, filename, output='./'):
        if filename not in self.metadata:
            return None
        else:
            nodes = self.metadata[filename]['nodes']
            self.metadata[filename]['modified'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            self.execute_on_nodes(nodes, self.execute_scp, args=(filename, output))
        return 1.0

    def list_files(self):
        return self.metadata
