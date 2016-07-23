class Mapper:
    def __init__(self):
        pass

    # map data by applying some data function
    # task_id - unique task_id
    # chunk_path - DFS path to the chunk file to map
    # map_script - DFS path to script of map function
    # reducers - list of all reducers with addresses
    def map(self, task_id, reducers, chunk_path, map_script):
        return "ok"

    def send_to_reducer(self, data, reducer):
        pass