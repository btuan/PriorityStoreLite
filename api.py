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
from threading import Thread


def psl_worker(psl, task_queue):
    while True:
        command = task_queue.get()
        if command is None:
            break
        func, args, kwargs = command
        func(*args, **kwargs)
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

    def persist_metadata(self):
        with open(self.config_dir + 'metadata.json', 'w') as f:
            json.dump(self.metadata, f)

    def execute_command(self, command, node):
        return run(['ssh', node, command])

    def create_file(self, filename, size=1048576, node=None, priority=0, persist=True):
        if filename in self.metadata:
            return None

        path = self.config['path'] + filename
        command = "head -c {} </dev/urandom > {}".format(size, path)

        if node is None:
            node = self.datanodes[randrange(len(self.datanodes))]
        elif node not in self.datanodes:
            return None

        self.metadata[filename] = {
            'node': node,
            'modified': time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
            'priority': priority,
        }
        if persist:
            self.persist_metadata()

        if filename.count('/') > 1:
            basename = filename[:filename.rfind('/')]
        else:
            basename = ''
        self.execute_command('mkdir -p "{}"'.format(self.config['path'] + basename), node)

        return self.execute_command(command, node)

    def delete_file(self, filename, persist=True):
        if filename not in self.metadata:
            return None

        command = "rm -rf {}".format(self.config['path'] + filename)
        node = self.metadata[filename]['node']
        del self.metadata[filename]

        if persist:
            self.persist_metadata()
        return self.execute_command(command, node)

    def retrieve_file(self, filename, output='./'):
        if filename not in self.metadata:
            return None
        else:
            node = self.metadata[filename]['node']
            self.metadata[filename]['modified'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        return run(['scp', node + ':' + self.config['path'] + filename, output])

    def list_files(self):
        return self.metadata

    def submit_tasks(self, task_list, block=False):
        """ Asynchronously process tasks in a task list, given as (func, args, kwargs). """
        num_workers = cpu_count()
        threads = []
        task_queue = queue.Queue()
        for _ in range(num_workers):
            t = Thread(target=psl_worker, args=[self, task_queue])
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


