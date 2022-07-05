import os
import random
import configparser
from typing import TypeVar

T = TypeVar("T", dict, configparser.SectionProxy)


BASEDIR = os.path.dirname(os.path.abspath(__file__))
INI_FILENAME = '.env'
SEP = '.'


class Args:

    instance = None
    _ini_read = None

    def __new__(cls, *args, **kwargs):
        if cls.instance is not None:
            return cls.instance
        else:
            cls._ini_read = configparser.ConfigParser()
            ini_path = os.sep.join([BASEDIR, INI_FILENAME])
            cls._ini_read.read(ini_path)
            return super(Args, cls).__new__(cls, *args, **kwargs)

    def __getitem__(self, item) -> T:

        try:
            key_1, key_2 = item.split(SEP)
            result = self.get_value(key_1)[key_2]
        except KeyError:
            if not os.getenv(item):
                raise AttributeError("item not found in .env file or env")
            result =  os.getenv(item)
        return result

    @classmethod
    def get_value(cls, k):
        return cls._ini_read[k]


def random_str(length=5):
    return "".join(random.sample("zyxwvutsrqponmlkjihgfedcba", length))


env = Args()
