import os

from enums import Status


class FakeFS:
    def __init__(self, base_dir="/var/fake_fs"):
        self.base_dir = base_dir

    def get_chunk(self, path):
        full_path = self.base_dir + path
        if not os.path.isfile(full_path):
            return {'status': Status.not_found}

        data = None
        with open(full_path, 'r') as f:
            data = f.read()

        return {'status': Status.ok, 'data': data}

    def download_to(self, v_path, l_path):
        full_path = self.base_dir + v_path
        if not os.path.isfile(full_path):
            return {'status': Status.not_found}

        data = None
        with open(full_path, 'r') as f:
            data = f.read()

        os.makedirs(os.path.dirname(l_path), exist_ok=True)
        with open(l_path, "w") as f:
            f.write(data)

        return {'status': Status.ok}

    def save(self, data, path):
        full_path = self.base_dir + path

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w+') as f:
            f.write(data)

        return {'status': Status.ok}