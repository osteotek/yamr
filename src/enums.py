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
    loaded_chunk = 202  # data chunk has been loaded

    error = 500
    already_exists = 501 # task alread exists
