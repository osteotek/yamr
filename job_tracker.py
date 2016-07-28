#!/usr/bin/env python3
import os
import random
import sys
import yaml
import _thread
import socket
import uuid
import time
from datetime import datetime
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

from os.path import dirname

sys.path.append(dirname(dirname(__file__)))
from yadfs.client.client import Client
from enums import Status, TaskStatus, MapStatus


class Chunk:
    def __init__(self, path, status, mapper):
        self.path = path
        self.status = status
        self.mapper = mapper
        self.map_path = ""


class Task:
    def __init__(self, input, map_script, reduce_script, chunks):
        self.input = input
        self.map_script = map_script
        self.reduce_script = reduce_script
        self.chunks = []
        for chunk_path, _ in chunks.items():
            self.chunks.append(Chunk(chunk_path, MapStatus.accepted, ""))
        self.status = TaskStatus.accepted
        self.map_paths = []

    def get_chunk(self, chunk_path):
        for chunk in self.chunks:
            if chunk_path == chunk.path:
                return chunk

    def get_chunk_to_process(self):
        for chunk in self.chunks:
            if chunk.status == MapStatus.accepted:
                return chunk

        return None


class JobTracker:
    def __init__(self, dump_on=True):
        self.dfs = Client()
        self.dump_on = dump_on
        self.dump_path = "./job_tracker.yml"
        self.workers = {}
        self.tasks = {}
        self.worker_timeout = 2
        self.queue = []
        self.free_workers = []

    # start job tracker
    def start(self):
        self._load_dump()
        _thread.start_new_thread(self.worker_watcher, ())

    def _load_dump(self):
        pass

    # get heartbeat from worker
    def heartbeat(self, worker_addr):
        if worker_addr not in self.workers:
            print('register worker ' + worker_addr)

        self.workers[worker_addr] = datetime.now()
        self.free_workers.append(worker_addr)
        return {'status': Status.ok}

    def _is_alive_worker(self, w_addr):
        if w_addr not in self.workers:
            return False

        last_hb = self.workers[w_addr]
        now = datetime.now()
        diff = (now - last_hb).total_seconds()
        return diff <= self.worker_timeout

    def worker_watcher(self):
        while 1:
            for w_name in list(self.workers):
                if not self._is_alive_worker(w_name):
                    print('Worker ', w_name, ' detected as not alive')
                    self.workers.pop(w_name)
        time.sleep(1)

    def create_task(self, input, map_script, reduce_script):
        input_info = self.dfs.path_status(input)
        task_id = uuid.uuid4()
        self.tasks[task_id] = Task(input, map_script, reduce_script, input_info['chunks'])
        self._start_task(task_id)
        return "ok"
    
    def _start_task(self, task_id):
        task = self.tasks[task_id]
        # wn = 0
        # for chunk in task.chunks:
        #     if wn > len(self.workers):
        #         wn = 0
        #     mapper = ServerProxy(self.workers.keys()[wn])
        #     mapper.map(task_id, len(self.workers), chunk.path, task.map_script)
        #     chunk.mapper = self.workers.keys()[wn]
        #     chunk.status = MapStatus.chunk_loaded
        #     wn = wn + 1
        
        task.status = TaskStatus.mapping
        _thread.start_new_thread(self._handle_map, task_id)

    def get_free_worker(self):
        if len(self.free_workers) == 0:
            return None
        worker = self.free_workers[0]
        self.free_workers.remove(worker)
        return worker

    def _handle_map(self, task_id):
        task = self.tasks[task_id]
        chunk = task.get_chunk_to_process()
        status = task.status
        if chunk is not None:
            while status != TaskStatus.mapping_done:
                worker_addr = self.get_free_worker()
                if worker_addr is not None:
                    worker = ServerProxy(worker_addr)
                    worker.map(task_id, len(self.workers), chunk.path, task.map_script)
                    chunk.mapper = worker_addr
                    chunk.status = MapStatus.chunk_loaded

                status = task.status

    # RPC call from mapped when a task is done:
    # mapper_addr: address of a mapper
    # task_id: id of task completed map
    # chunk_path: path of a chunk being mapped
    def mapping_done(self, mapper_addr, task_id, chunk_path):
        if task_id not in self.tasks:
            return {"status": Status.not_found}

        task = self.tasks[task_id]
        chunk = task.get_chunk(chunk_path)
        chunk.status = MapStatus.map_applied

        map_completed = self._check_task_status(task_id)

        self.free_workers.append(mapper_addr)

        if map_completed:
            task.status = TaskStatus.mapping_done
            _thread.start_new_thread(self._handle_reduce, task_id)

    def _handle_reduce(self, task_id):
        task = self.tasks[task_id]


    # RPC call from reducer that is done
    # addr - reducer addr
    # task_id - unique task_id
    # region - number of task which was completed
    def reducing_done(self, addr, task_id, region):
        return {"status": Status.ok}

    def _check_task_status(self, task_id):
        task = self.tasks[task_id]
        for chunk in task.chunks:
            if chunk.status != MapStatus.map_applied:
                return False

        return True

    def get_status(self, input):
        pass


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

    server = SimpleXMLRPCServer((host, port), logRequests=True, allow_none=True)
    server.register_introspection_functions()
    server.register_instance(jt)
    server.serve_forever()
