class BaseMapper:
    def __init__(self):
        self.tuples = []

    def run_map(self, data):
        self.tuples = []
        self.map(data)
        return self.tuples

    def map(self, data):
        pass

    def emit(self, key, value):
        self.tuples.append((key, value))


class WordCountMapper(BaseMapper):
    def map(self, data):
        if data is None:
            return

        for word in data.strip().split(' '):
            word = word.strip(',.')
            # exclude empty strings from keys
            if len(word) > 0:
                self.emit(word, 1)