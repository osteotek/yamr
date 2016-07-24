class Reducer:
    def __init__(self):
        pass

    # saves mapped key-values data from mapper
    # mapper_id - unique mapper id
    # task_id - unique task id
    # data should be saved in task_id folder
    def save_mapped_data(self, mapper_id, task_id, data):
        pass

    # signal from JT for starting reducing
    def reduce(self, task_id):
        pass

    # saves reduced result to dfs
    def save_result_to_dfs(self, task_id, result):
        pass