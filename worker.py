#!/usr/bin/env python3
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from mapper import Mapper
from reducer import Reducer, RPCMapperClient
import sys
import _thread
import time
import cfg
import fake_fs
import yadfs.client.client


class Worker:
    def __init__(self, fs, name, addr, opts):
        self.addr = addr
        self.jt_addr = opts["jt_addr"]
        self.jt = ServerProxy(self.jt_addr)
        self.hb_timeout = 0.5  # heartbeat timeout in seconds
        self.on = True
        self.mapper = Mapper(opts, fs, "map" + name, addr)
        self.reducer = Reducer(fs, "reduce" + name, addr, opts, RPCMapperClient())

    def start(self):
        print('Init worker')
        print('Start sending heartbeats to', self.jt_addr)
        _thread.start_new_thread(self._heartbeat, ())
        print('Server is ready')

    def _heartbeat(self):
        while self.on:
            try:
                self.jt.heartbeat(self.addr)
            except Exception as e:
                print(e)
            time.sleep(self.hb_timeout)

    # map data by applying some data function
    # task_id - unique task_id
    # reducers_count - number of reducers for the task
    # chunk_path - DFS path to the chunk file to map
    # map_script - DFS path to script of map function
    # restart_task - if True then restart map task even its already completed or executing now
    def map(self, task_id, rds_count, chunk_path, map_script, restart_task=False):
        return self.mapper.map(task_id, rds_count, chunk_path, map_script, restart_task)

    # get status of task execution for the current task
    def get_status(self, task_id, chunk_path):
        return self.mapper.get_status(task_id, chunk_path)

    # read mapped data for specific region
    # task_id - unique task_id
    # region - is a integer region which is specified for the current reducer
    # Return dict {status: Status.ok, data: list of tuples}
    # if file not exists then status = Status.not_found
    # if file is empty then returns ok and empty list
    def read_mapped_data(self, task_id, region_number):
        return self.mapper.read_mapped_data(task_id, region_number)

if __name__ == '__main__':
    port = int(sys.argv[1])
    addr = "http://localhost:" + str(port)

    cfg_path = sys.argv[2]
    opts = cfg.load(cfg_path)

    fs = yadfs.client.client.Client()
    worker = Worker(fs, str(port), addr, opts)
    worker.start()

    server = SimpleXMLRPCServer(("localhost", port))
    server.register_introspection_functions()
    server.register_instance(worker)
    server.serve_forever()
