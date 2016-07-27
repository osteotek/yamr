#!/usr/bin/env python3
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class ReduceTask:
    def __init__(self, task_id, region, mappers):
        self.task_id = task_id
        self.region = region
        self.mappers = mappers

class Reducer:
    def __init__(self, fs, name, addr, opts):
        self.fs = fs
        self.name = name
        self.addr = addr
        self.job_tracker = ServerProxy(opts["JobTracker"]["address"])

    # signal from JT for starting reducing
    # task_id - unique task_id
    # region for which reducer is responsible
    # mappers which contain data for current task
    def reduce(self, task_id, region, mappers):
        pass


    def get_status(self, task_id):
        pass

    # saves reduced result to dfs
    def save_result_to_dfs(self, task_id, result):
        pass