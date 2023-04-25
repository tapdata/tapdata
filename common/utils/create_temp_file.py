import os
from importlib import import_module


class ManageTempFile:

    _default_file_path = os.path.dirname(os.path.abspath(__file__)) + '/../../test_cases/temp.py'

    def __init__(self, file: str = None, content: str = ""):

        self.file = self._default_file_path if file is None else file
        self.content = content

    def __enter__(self):
        with open(self.file, "w+", encoding="utf-8") as f:
            f.write(self.content)
        if self.file.endswith(".py"):
            module = import_module(os.path.basename(self.file).split(".")[0])
            return module.test

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.file)
        return True
