import ast

# this task count maximum year temp
# it requires input as "(201604, 32.5),

class Mapper:
    def __init__(self):
        self.tuples = None

    def run_map(self, data):
        self.tuples = []
        self.map(data)
        return self.tuples

    def map(self, data):
        if data is None:
            return
        tuples = ast.literal_eval(data)

        for t in tuples:
            year = int(str(t[0])[:4])
            self.emit(year, t[1])

    def emit(self, key, value):
        self.tuples.append((key, value))


class Reducer:
    def __init__(self):
        self.tuples = None

    def run_reduce(self, data):
        self.tuples = []
        combined = self.combine_data(data)
        for t in combined:
            self.reduce(t[0], t[1])
        return self.tuples

    @staticmethod
    def combine_data(data):
        if len(data) == 0:
            return {}

        result = []

        curr_key = data[0][0]
        new_t = (curr_key, [])
        result.append(new_t)

        for t in sorted(data, key=lambda x: x[0]):
            key = t[0]
            val = t[1]

            if curr_key != key:
                curr_key = key
                new_t = (curr_key, [])
                result.append(new_t)

            new_t[1].append(val)

        return result

    def reduce(self, key, values):
        s = max(x for x in values)
        self.emit(key, s)

    def emit(self, key, value):
        self.tuples.append((key, value))
