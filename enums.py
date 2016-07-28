class Status:
    ok = 200
    not_found = 404
    error = 500
    already_exists = 409

    @staticmethod
    def description(stat):
        if stat == Status.ok:
            return "Ok"
        elif stat == Status.not_found:
            return "Item not found"
        elif stat == Status.error:
            return "Internal error"
        else:
            return "Item already exists"


class MapStatus:
    accepted = 201  # task has been started
    chunk_loaded = 202  # data chunk has been loaded
    mapper_loaded = 205
    map_applied = 210 # map function was aplied to the input record
    partitions_saved = 220

    chunk_not_found = 404

    error = 500
    map_script_not_found = 504
    map_script_loading_error = 505
    exec_map_error = 510
    save_partitions_err = 520
    already_executed = 501  # task already exists

    finished = 600


class TaskStatus:
    accepted = 200
    mapping = 210
    mapping_done = 220
    reducing = 230
    task_done = 240


class ReduceStatus:
    accepted = 201  # task has been started

    reduce_not_found = 404

    finished = 600



