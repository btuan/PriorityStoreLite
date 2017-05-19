""" api.py

Python API for PriorityStoreLite. Built on top of commonly available UNIX tools.

Exposes a simple CRUD API. Assumes that user to which SSH is done to has public key SSH configured.

Author: Brian Tuan
"""

import os
import json
from random import randrange
from subprocess import run
import time


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
            self.datanodes = json.load(f)
            assert(len(self.datanodes) != 0)

    def persist_metadata(self):
        with open(self.config_dir + 'metadata.json', 'w') as f:
            json.dump(self.metadata, f)

    def execute_command(self, command, node):
        return run(['ssh', node, command])

    def create_file(self, filename, size=1048576, node=None):
        if filename in self.metadata:
            return None

        path = self.config['path'] + filename
        command = "head -c {} </dev/urandom > {}".format(size, filename)

        if node is None:
            node = self.datanodes[randrange(len(self.datanodes))]
        elif node not in self.datanodes:
            return None

        self.metadata[filename] = {'node': node, 'modified': time.time()}
        self.persist_metadata()

        if '/' in filename:
            basename = filename[:filename.rfind('/')]
        else:
            basename = ''
        self.execute_command('mkdir -p "{}"'.format(self.config['path'] + basename), node)

        return self.execute_command(command, node)

    def delete_file(self, filename):
        if filename not in self.metadata:
            return None

        command = "rm -rf filename"
        node = self.metadata[filename]
        del self.metadata[filename]

        self.persist_metadata()
        return self.execute_command(command, node)

    def retrieve_file(self, filename, output='./'):
        if filename not in self.metadata:
            return None
        else:
            node = self.metadata[filename]['node']
            self.metadata[filename]['modified'] = time.time()
        return run(['scp', node + ':' + filename, output])

    def list_files(self):
        return self.metadata.keys()
