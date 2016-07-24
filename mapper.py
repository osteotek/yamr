import _thread
import sys
import uuid

from fake_fs import FakeFS

from enums import MapStatus, Status

BASE_DIR = "/etc/yamr/"


class MapTask:
    def __init__(self, rds_count, chunk_path, map_script):
        self.status = MapStatus.accepted
        self.rds_count = rds_count
        self.chunk_path = chunk_path
        self.script_path = map_script

    @property
    def in_progress(self):
        return self.status != MapStatus.chunk_not_found and self.status != MapStatus.error \
               and self.status != MapStatus.finished


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

        self.tasks[task_id] = MapTask(reducers_count, chunk_path, map_script)
        _thread.start_new_thread(self.process_task, (task_id,))

        return {'status': MapStatus.accepted}


    def process_task(self, task_id):
        task = self.tasks[task_id]
        r = self.fs.get_chunk(task.chunk_path)
        if r['status'] == Status.not_found:
            task.status = MapStatus.chunk_not_found
            return

        data = r['data']
        task.status = MapStatus.chunk_loaded



        #task.status = MapStatus.finished


    def get_status(self, task_id):
        t = self.tasks[task_id]
        return {'status': t.status, 'in_progress': t.in_progress}

    # read mapped data for specific region
    # task_id - unique task_id
    # region - is a region which is specified for the current reducer
    def read_mapped_data(self, task_id, region):
        pass

if __name__ == '__main__':
    name = sys.argv[1]
    port = int(sys.argv[2])

    fs = FakeFS()  # use fake fs for local development
    fs.save("ho ho ho", "/my_folder/chunk")

    mapper = Mapper(fs, name)
    task_id = uuid.uuid4()
    r = mapper.map(task_id, 4, "/my_folder/chunk", "some")

    while mapper.get_status(task_id)['status'] != MapStatus.chunk_loaded:
        pass

    print("data loaded")


