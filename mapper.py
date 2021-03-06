import _thread
import sys
import uuid
import os
import json
import cfg
import importlib.util
from hash_partitioner import HashPartitioner
from xmlrpc.client import ServerProxy

from fake_fs import FakeFS
from enums import MapStatus, Status


# unique map task for one chunk
class MapTask:
    def __init__(self, task_id, rds_count, chunk_path, map_script):
        self.task_id = task_id
        self.status = MapStatus.accepted
        self.rds_count = rds_count
        self.chunk_path = chunk_path
        self.script_path = map_script

    @property
    def in_progress(self):
        return self.status != MapStatus.chunk_not_found and self.status != MapStatus.error \
               and self.status != MapStatus.finished


class Mapper:
    def __init__(self, opts, fs, name, my_addr):
        self.name = name
        self.my_addr = my_addr
        self.opts = opts
        self.fs = fs  # client to dfs
        self.work_dir = opts["base_dir"] + name
        self.tasks = {}
        self.hasher = HashPartitioner()
        self.job_tracker = ServerProxy(opts["jt_addr"])

    def log(self, task_id, msg):
        print("Task", task_id, ":", msg)

    def err(self, task_id, msg, e=None):
        print("Task", task_id, ":", msg, e, file=sys.stderr)

    # map data by applying some data function
    # task_id - unique task_id
    # reducers_count - number of reducers for the task
    # chunk_path - DFS path to the chunk file to map
    # map_script - DFS path to script of map function
    # restart_task - if True then restart map task even its already completed or executing now
    def map(self, task_id, rds_count, chunk_path, map_script, restart_task=False):
        print("Map request - task_id:", task_id, "rdc_count:", rds_count, "chunk_path:", chunk_path,
              "map_script:", map_script, "restart_task:", restart_task)

        # if task_id in self.tasks and chunk_path in self.tasks[task_id] and not restart_task:
        #     self.log(task_id, "Task with the same id is already exists.")
        #     return {'status': MapStatus.already_executed}

        if task_id not in self.tasks:
            self.tasks[task_id] = {}

        self.tasks[task_id][chunk_path] = MapTask(task_id, rds_count, chunk_path, map_script)
        _thread.start_new_thread(self.process_task, (self.tasks[task_id][chunk_path],))

        return {'status': MapStatus.accepted}

    def process_task(self, task):
        task_id = task.task_id
        self.log(task_id, "start task")
        self.log(task_id, "trying to load chunk " + task.chunk_path)

        r = self.fs.get_chunk(task.chunk_path)
        if r['status'] == Status.not_found:
            task.status = MapStatus.chunk_not_found
            return

        self.log(task_id, "chunk " + task.chunk_path + " has been loaded")
        task.status = MapStatus.chunk_loaded
        mapper = self.load_mapping_script(task)

        if task.status == MapStatus.mapper_loaded:
            tuples = self.exec_mapping(mapper, task, r['data'])
            if task.status == MapStatus.map_applied:
                regions = self.partition(task.rds_count, tuples)
                self.save_partitions(task, task.chunk_path, regions)

                if task.status == MapStatus.partitions_saved:
                    task.status = MapStatus.partitions_saved
                    self.send_mapping_done(task_id, task.chunk_path)
                    task.status = MapStatus.finished

    def load_mapping_script(self, task):
        try:
            l_path = self.work_dir + "/" + str(task.task_id) + "/map.py"
            r = self.fs.download_to(task.script_path, l_path)
            if r['status'] == Status.not_found:
                task.status = MapStatus.map_script_not_found
                return None

            spec = importlib.util.spec_from_file_location("map"+str(task.task_id), l_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            task.status = MapStatus.mapper_loaded
            return mod.Mapper()
        except Exception as e:
            self.err(task.task_id, "error during script execution", e)
            task.status = MapStatus.map_script_loading_error
            return None

    def exec_mapping(self, mapper, task, data):
        try:
            self.log(task.task_id, "start mapping execution")
            tuples = mapper.run_map(data)
            self.log(task.task_id, "mapping function completed, tuples count - " + str(len(tuples)))
            task.status = MapStatus.map_applied
            return tuples
        except Exception as e:
            self.err(task.task_id, "Error during executing map script for chunk " + task.chunk_path, e)
            task.status = task.status = MapStatus.exec_map_error
            return []

    def partition(self, rds_count, tuples):
        regions = {}
        for i in range(1, rds_count + 1):
            regions[i] = []

        for t in tuples:
            r = self.hasher.get_partition(t[0], t[1], rds_count)
            regions[r+1].append(t)

        for k,v in regions.items():
            v.sort(key=lambda tup: tup[0])

        return regions

    def _get_chunk_dir_path(self, task_id, chunk_path):
        return self.work_dir + "/" + str(task_id) + chunk_path

    # save file in path /base_dir/task_id/chunk_path/1 where 1 is a region number
    # task_id - unique task_id
    # chunk_path - path of the processed_chunk
    # dictionary of mapped data which is sorted to regions
    def save_partitions(self, task, chunk_path, regions):
        try:
            task_dir = self._get_chunk_dir_path(task.task_id, chunk_path)
            self.log(task.task_id, "save map result of " + chunk_path + " to " + task_dir)
            if not os.path.exists(task_dir):
                os.makedirs(task_dir)

            for k, v in regions.items():
                path = task_dir + "/" + str(k)
                with open(path, 'w') as file:
                    file.write(json.dumps(v))
            task.status = MapStatus.partitions_saved

        except Exception as e:
            self.log(task_id, "Error during saving mapped partitions for chunk " + chunk_path, e)
            task.status = MapStatus.save_partitions_err

    def send_mapping_done(self, task_id, chunk_path):
        try:
            self.job_tracker.mapping_done(self.my_addr, str(task_id), chunk_path)
            self.log(task_id, "Sent message to job tracker about finishing mapping of " + chunk_path)
        except Exception as e:
            self.err(task_id, "Failed to send result for chunk " + chunk_path, e)

    def get_status(self, task_id, chunk_path):
        if task_id not in self.tasks and chunk_path not in self.tasks[task_id]:
            return {'status': MapStatus.chunk_not_found}

        t = self.tasks[task_id][chunk_path]
        return {'status': t.status}

    # read mapped data for specific region
    # task_id - unique task_id
    # region - is a integer region which is specified for the current reducer
    # Return dict {status: Status.ok, data: list of tuples}
    # if file not exists then status = Status.not_found
    # if file is empty then returns ok and empty list
    def read_mapped_data(self, task_id, region_number):
        try:
            self.log(task_id, "request to load region " + str(region_number))

            if task_id not in self.tasks:
                return {'status': Status.not_found, 'data': []}

            result = []

            for chunk_path in self.tasks[task_id]:
                path = self._get_chunk_dir_path(task_id, chunk_path)
                path += "/" + str(region_number)

                if not os.path.isfile(path):
                    self.log(task_id, "chunk " + path + " is not found in mapped data")
                    continue

                with open(path, "r") as f:
                    list = json.loads(f.read())

                result.extend([(x[0], x[1]) for x in list])

            self.log(task_id, "Send to reducer data for region " + str(region_number))
            return {'status': Status.ok, 'data': result}
        except Exception as e:
            self.log(task_id, "Error during loading region " + str(region_number) + ": " + str(e))
            return {'status': Status.error, 'data': []}

if __name__ == '__main__':
    name = sys.argv[1]
    port = int(sys.argv[2])
    cfg_path = sys.argv[3]
    script_path = sys.argv[4]
    rds_count = 5

    chunk = "/my_folder/chunk"
    fs = FakeFS()  # use fake fs for local development
    fs.save("[[201501, 31.2],[201307, 32],[201302, 31.2],[201301, 21.2],[201407, 21.2],[201302, 11.2],[201002, 20],\
            [201402, 30.2],[201304, 25],[201305, 11.2],[201601, 13.2],[201605, 15.2],[201502, 18.2],[201003, 20]]", chunk)

    with open(script_path, "r") as file:
        data = file.read()
        fs.save(data, "/scripts/max_temp.py")

    opts = cfg.load(cfg_path)
    print("JT address", opts["jt_addr"])

    mapper = Mapper(opts, fs, name, "http://localhost:" + str(port))
    task_id = uuid.uuid4()
    r = mapper.map(task_id, rds_count, chunk, "/scripts/max_temp.py")

    while mapper.get_status(task_id, chunk)['status'] != MapStatus.finished:
        pass

    i = 1
    while i <= 4:
        print(i, "region:", mapper.read_mapped_data(task_id, i))
        i += 1


