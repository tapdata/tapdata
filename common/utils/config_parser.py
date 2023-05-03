import os
import configparser
from typing import Union
from log import logger


BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + '/../../config'
CONFIG_FILENAME = "init_data_conf.ini"
DEFAULT_CONFIG = os.path.sep.join([BASE_DIR, CONFIG_FILENAME])
SEP = "."


class Config:

    _config: configparser.ConfigParser = None

    def __new__(cls, *args):
        cls._config = configparser.ConfigParser()
        ini_path = os.sep.join([BASE_DIR, CONFIG_FILENAME])
        if os.path.exists(ini_path):
            cls._config.read(ini_path)
        else:
            cls._config.read(DEFAULT_CONFIG)
        return super(Config, cls).__new__(cls, *args)

    def __init__(self, *args):
        self.sections = self._config.sections()

    def __getitem__(self, item: Union[int, str]):
        result = None
        if isinstance(item, str):
            try:
                key_1, key_2 = item.split(SEP)
                result = self.get_value(key_1)[key_2]
            except KeyError:
                if not os.getenv(item):
                    raise AttributeError("item not found in *.ini file or env")
                result = os.getenv(item)
        elif isinstance(item, int):
            item = self._config.items(self._config.sections()[item])
            result = dict(item)
        return result

    @classmethod
    def get_value(cls, k):
        return cls._config[k]

    @classmethod
    def update_value(cls, section: str, options: str, value: str) -> bool:
        try:
            cls._config.set(section, options, str(value))
        except Exception as e:
            logger.error("{}", e)
            return False
        return True


config: Config = Config()

if __name__ == '__main__':
    print(config["column100.name"])
    config.update_value("column100", "initial_totals", 600000)
    for i in config:
        print(i)
