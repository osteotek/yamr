import os
from src.enums import Status


class FakeFS:
    def __init__(self, base_dir="/var/fake_fs"):
        self.base_dir = base_dir

    def get_chunk(self, path):
        full_path = self.base_dir + path
        if not os.path.isfile(full_path):
            return {'status': Status.not_found }

        data = None
        with open(full_path, 'r') as f:
            data = f.read()

        return data

    def save(self, data, path):
        full_path = self.base_dir + path

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w+') as f:
            f.write(data)

        return {'status': Status.ok}