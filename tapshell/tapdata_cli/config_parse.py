import os
import configparser
import sys
from typing import TypeVar


BASE_DIR = os.environ.get("PROJECT_PATH")
CONFIG_FILENAME = "config.ini"
DEFAULT_CONFIG = os.path.sep.join([os.path.dirname(__file__), "default.ini"])
SEP = "."
T = TypeVar("T", dict, configparser.SectionProxy, str)


class Config:

    instance = None
    _config: configparser.ConfigParser = None

    def __new__(cls, *args, **kwargs):
        if cls.instance is not None:
            return cls.instance
        else:
            cls._config = configparser.ConfigParser()
            ini_path = os.sep.join([BASE_DIR, CONFIG_FILENAME])
            if os.path.exists(ini_path):
                cls._config.read(ini_path)
            else:
                cls._config.read(DEFAULT_CONFIG)
            return super(Config, cls).__new__(cls, *args, **kwargs)

    def __getitem__(self, item) -> T:

        try:
            key_1, key_2 = item.split(SEP)
            result = self.get_value(key_1)[key_2]
        except KeyError:
            if not os.getenv(item):
                raise AttributeError("item not found in config.ini file or env")
            result = os.getenv(item)
        return result

    @classmethod
    def get_value(cls, k):
        return cls._config[k]
