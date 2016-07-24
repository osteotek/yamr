#!/usr/bin/env python3
import os
import random
import sys
import yaml
import _thread
import socket
import uuid
from datetime import datetime
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

from os.path import dirname

sys.path.append(dirname(dirname(__file__)))
from yadfs.client.client import Client
from enums import Status, TaskStatus, MapStatus


class Task:
    def __init__(self, input, map_script, reduce_script, chunks):
        self.input = input
        self.map_script = map_script
        self.reduce_script = reduce_script
        self.chunks = []
        for chunk_path, _ in chunks.items():
            self.chunks.append({
                'chunk_path': chunk_path,
                'mapper': '',
                'status': MapStatus.accepted
            })
        self.status = TaskStatus.accepted


class JobTracker:
    def __init__(self, dump_on=True):
        self.dfs = Client()
        self.dump_on = dump_on
        self.dump_path = "./job_tracker.yml"
        self.workers = {}
        self.tasks = {}

    # start job tracker
    def start(self):
        self._load_dump()

    def _load_dump(self):
        pass

    def create_task(self, input, map_script, reduce_script):
        input_info = self.dfs.path_status(input)
        task_id = uuid.uuid4()
        self.tasks[task_id] = Task(input, map_script, reduce_script, input_info['chunks'])
        self._start_task(task_id)
        return "ok"
    
    def _start_task(self, task_id):
        task = self.tasks[task_id]
        wn = 0
        for chunk in task.chunks:
            if wn > len(self.workers):
                wn = 0
            mapper = ServerProxy(self.workers.keys()[wn])
            mapper.map(task_id, len(self.workers), chunk['chunk_path'], task.map_script)
            chunk['mapper'] = self.workers.keys()[wn]
            chunk['status'] = MapStatus.chunk_loaded
            wn = wn + 1
        
        task.status = TaskStatus.mapping

    def mapping_done(self):
        pass

    def get_status(self, input):
        pass

    # get heartbeat from worker
    def heartbeat(self, worker_addr):
        if worker_addr not in self.workers:
            print('register worker ' + worker_addr)

        self.workers[worker_addr] = datetime.now()
        return {'status': Status.ok}

# args: host and port: localhost 11111
if __name__ == '__main__':
    if len(sys.argv) == 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
    else:
        host = 'localhost'
        port = 11111

    jt = JobTracker(dump_on=True)
    jt.start()

    server = SimpleXMLRPCServer((host, port), logRequests=False, allow_none=True)
    server.register_introspection_functions()
    server.register_instance(jt)
    server.serve_forever()
