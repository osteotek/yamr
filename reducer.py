#!/usr/bin/env python3
import _thread
import sys
import cfg
import uuid
from xmlrpc.client import ServerProxy
from enums import ReduceStatus, Status
from fake_fs import FakeFS
import map_libs.word_count
import os
import json

BASE_DIR = "/etc/yamr/"

# fake mapper client for testing
# communication reducer and mapper
class FakeMapperClient:
    def __init__(self):
        self.data = {}

    def load_mapped_data(self, map_addr, task_id, region):
        if map_addr not in self.data and task_id not in self.data[map_addr] \
                and region not in self.data[map_addr][task_id]:
            return {'status': Status.not_found}

        return self.data[map_addr][task_id][region]

    def put(self, map_addr, task_id, region, data):
        if map_addr not in self.data:
            self.data[map_addr] = {}

        if task_id not in self.data[map_addr]:
            self.data[map_addr][task_id] = {}

        self.data[map_addr][task_id][region] = data


class ReduceTask:
    def __init__(self, task_id, region, mappers, script_path):
        self.task_id = task_id
        self.region = region
        self.mappers = mappers
        self.status = ReduceStatus.accepted
        self.script_path = script_path


class Reducer:
    def __init__(self, fs, name, addr, opts, mapper_cl):
        self.fs = fs
        self.name = name
        self.addr = addr
        self.job_tracker = ServerProxy(opts["JobTracker"]["address"])
        self.tasks = {}
        self.mapper_cl = mapper_cl  # client for loading data from mappers
        self.work_dir = BASE_DIR + name

    def log(self, task_id, msg):
        print("Task", task_id, ":", msg)

    def err(self, task_id, msg, e=None):
        print("Task", task_id, ":", msg, e, file=sys.stderr)

    # signal from JT for starting reducing
    # task_id - unique task_id
    # region for which reducer is responsible
    # mappers which contain data for current task
    # path in DFS to files
    def reduce(self, task_id, region, mappers, script_path):
        self.log(task_id, "Get request for start reducing of region " + str(region))
        if task_id not in self.tasks:
            self.tasks[task_id] = {}

        task = ReduceTask(task_id, region, mappers, script_path)
        self.tasks[task_id][region] = task
        _thread.start_new_thread(self._process_reduce_task, (task, ))
        return {'status': ReduceStatus.accepted}

    def _process_reduce_task(self, task):
        data = self._load_data_from_mappers(task)

        if task.status == ReduceStatus.data_loaded:
            result = self.execute_reduce_script(task, data)

            if task.status == ReduceStatus.data_reduced:
                self._save_result_to_dfs(task, result)

                if task.status == ReduceStatus.data_saved:
                    task.status = ReduceStatus.finished

    def _load_data_from_mappers(self, task):
        try:
            self.log(task.task_id, "Start loading data from mappers to region " + str(task.region))
            task.status = ReduceStatus.start_data_loading
            result = []
            for mapper in task.mappers:
                data = self.mapper_cl.load_mapped_data(mapper, task.task_id, task.region)
                result.extend(data)

            task.status = ReduceStatus.data_loaded
            return result
        except Exception as e:
            task.status = ReduceStatus.err_data_loading
            self.err(task.task_id, "Error during loading data for region " + str(region), e)

    def execute_reduce_script(self, task, data):
        try:
            self.log(task.task_id, "Start loading reducing script for executing " + task.script_path)
            reducer = map_libs.word_count.Reducer()
            task.status = ReduceStatus.data_reduced
            return reducer.run_reduce(data)
        except Exception as e:
            task.status = ReduceStatus.err_reduce_script
            self.err("Error during executing reducer script", e)

    # save reduced result to dfs
    def _save_result_to_dfs(self, task, result):
        try:
            path = "/" + str(task.task_id) + "/result/" + str(task.region)
            self.log(task.task_id, "Save result of region " + str(task.region) + " to " + path)
            fs.save(json.dumps(result), path)
            task.status = ReduceStatus.data_saved
        except Exception as e:
            task.status = ReduceStatus.err_save_result
            self.err(task_id, "Error during saving region " + str(task.region) + " to DFS")

    # get status of current reducer execution
    # task_id - unique task_id
    # region - regions of keys which reducer should reduce
    # returns dict {'status': ReduceStatus }
    def get_status(self, task_id, region):
        if task_id not in self.tasks and region not in self.tasks[task_id]:
            return {'status': ReduceStatus.reduce_not_found}

        t = self.tasks[task_id][region]
        return {'status': t.status}

if __name__ == '__main__':
    name = sys.argv[1]
    port = int(sys.argv[2])
    cfg_path = sys.argv[3]
    script_path = sys.argv[4]

    opts = cfg.load(cfg_path)
    print("JT address", opts["JobTracker"]["address"])
    fs = FakeFS()
    task_id = uuid.uuid4()

    region = 1

    mapper_cl = FakeMapperClient()
    mapper_cl.put("map1", task_id, region, [('a', 1), ('a', 1), ('a', 1), ('b', 1), ('b', 1)])
    mapper_cl.put("map2", task_id, region, [('a', 1), ('b', 1), ('b', 1), ('d', 1)])
    mapper_cl.put("map3", task_id, region, [('a', 1), ('d', 1)])

    reducer = Reducer(fs, name, "http://localhost:" + str(port), opts, mapper_cl)

    r = reducer.reduce(task_id, region, ["map1", "map2"], "/scripts/word_count.py")
    while reducer.get_status(task_id, region)['status'] != ReduceStatus.finished:
        pass

    reg_1 = fs.get_chunk("/"+str(task_id) + "/result/" + str(region))
    print('reduce has finished', reg_1)