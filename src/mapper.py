import sys
import os
import uuid
from src.enums import MapStatus
from src.fake_fs import FakeFS

BASE_DIR = "/etc/yamr/"


class MapTask:
    def __init__(self):
        self.status = MapStatus.accepted


class Mapper:
    def __init__(self, fs, name):
        self.name = name
        self.fs = fs  # client to dfs
        self.work_dir = BASE_DIR + name
        self.tasks = {}

    # map data by applying some data function
    # task_id - unique task_id
    # reducers_count - number of reducers for the task
    # chunk_path - DFS path to the chunk file to map
    # map_script - DFS path to script of map function
    # restart_task - if True then restart map task even its already completed or executing now
    def map(self, task_id, reducers_count, chunk_path, map_script, restart_task=False):

        if task_id in self.tasks and not restart_task:
            return {'status': MapStatus.already_exists}

        self.tasks[task_id] = MapTask()

        return {'status': MapStatus.accepted}

    # read mapped data for specific region
    # task_id - unique task_id
    # region - is a region which is specified for the current reducer
    def read_mapped_data(self, task_id, region):
        pass


if __name__ == '__main__':
    name = sys.argv[1]
    port = int(sys.argv[2])

    fs = FakeFS()  # use fake fs for local development
    mapper = Mapper(fs, name)

    task_id = uuid.uuid4()
    r = mapper.map(task_id, 4, "/my_folder/chunk", "some")
    print(r)