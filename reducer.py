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
import importlib.util
import json


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


class RPCMapperClient:
    def load_mapped_data(self, map_addr, task_id, region):
        cl = ServerProxy(map_addr)
        return cl.read_mapped_data(task_id, region)['data']


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
        self.job_tracker = ServerProxy(opts["jt_addr"])
        self.tasks = {}
        self.mapper_cl = mapper_cl  # client for loading data from mappers
        self.work_dir = opts["base_dir"] + name

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
            reducer = self._load_reduce_script(task)
            if task.status == ReduceStatus.reducer_loaded:
                result = self.execute_reduce_script(reducer, task, data)

                if task.status == ReduceStatus.data_reduced:
                    self._save_result_to_dfs(task, result)

                    if task.status == ReduceStatus.data_saved:
                        self._send_reducing_done(task)

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

    def _load_reduce_script(self, task):
        try:
            l_path = self.work_dir + "/" + str(task.task_id) + "/reduce.py"
            r = self.fs.download_to(task.script_path, l_path)
            if r['status'] == Status.not_found:
                task.status = ReduceStatus.reduce_script_not_found
                return None

            spec = importlib.util.spec_from_file_location("reduce" + str(task.task_id), l_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            task.status = ReduceStatus.reducer_loaded
            return mod.Reducer()
        except Exception as e:
            self.err(task.task_id, "error during script execution", e)
            task.status = ReduceStatus.err_reducer_loading
            return None

    def execute_reduce_script(self, reducer, task, data):
        try:
            self.log(task.task_id, "Start loading reducing script for executing " + task.script_path)
            r = reducer.run_reduce(data)
            task.status = ReduceStatus.data_reduced
            return r
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

    def _send_reducing_done(self, task):
        try:
            self.job_tracker.reducing_done(self.addr, str(task.task_id), task.region)
            self.log(task.task_id, "Sent message to job tracker about finishing reducing of region " + str(task.region))
            task.status = ReduceStatus.finished
        except Exception as e:
            task.status = ReduceStatus.err_send_done
            self.err(task.task_id, "Failed to send result to JT for region " + str(task.region), e)

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
    print("JT address", opts["jt_addr"])
    fs = FakeFS()
    task_id = uuid.uuid4()

    with open(script_path, "r") as file:
        data = file.read()
        fs.save(data, "/scripts/word_count.py")

    region = 1

    mapper_cl = FakeMapperClient()
    mapper_cl.put("map1", task_id, region, [('a', 1), ('a', 1), ('a', 1), ('b', 1), ('b', 1)])
    mapper_cl.put("map2", task_id, region, [('a', 1), ('b', 1), ('b', 1), ('d', 1)])
    mapper_cl.put("map3", task_id, region, [('a', 1), ('d', 1)])

    reducer = Reducer(fs, name, "http://localhost:" + str(port), opts, mapper_cl)
    r = reducer.reduce(task_id, region, ["map1", "map2"], "/scripts/word_count.py")
    while reducer.get_status(task_id, region)['status'] != ReduceStatus.err_send_done:
        pass

    reg_1 = fs.get_chunk("/"+str(task_id) + "/result/" + str(region))
    print('reduce has finished', reg_1)
