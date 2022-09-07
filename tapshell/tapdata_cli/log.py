import time
import logging

from tapdata_cli.config_parse import Config

config = Config()

logger_header = False

# Global Logger Utils
def get_log_level(level):
    levels = {
        "debug": 0,
        "info": 10,
        "notice": 20,
        "warn": 30,
        "error": 40,
    }
    if level.lower() in levels:
        return levels[level.lower()]
    return 100


class Logger:
    def __init__(self, name=""):
        self.name = name
        self.max_len = 0
        self.logger_header = False
        self.color_map = {
            "info": "32",
            "notice": "34",
            "debug": "0",
            "warn": "33",
            "error": "31"
        }

    def _header(self):
        if self.logger_header:
            return "\033[1;34m" + self.name + ", " + "\033[0m" + \
                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": "
        else:
            return ""

    def _print(self, msg, **kwargs):
        wrap = kwargs.get("wrap", True)
        end = "\r"
        if wrap:
            end = "\n"
        l = len(self._header() + msg)
        tail = ""
        if l > self.max_len:
            self.max_len = l
        if self.max_len > 180:
            self.max_len = 180
        if l < self.max_len:
            tail = " " * (self.max_len - l)
        print(self._header() + msg + tail, end=end)

    def info(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0].replace("{}", "\033[1;32m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def debug(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0]
        self._print(msg.format(*args[1:]), **kargs)

    def notice(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0].replace("{}", "\033[1;34m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def warn(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0].replace("{}", "\033[1;33m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def error(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0].replace("{}", "\033[1;31m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def fatal(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0].replace("{}", "\033[1;31m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def log(self, *args, **kargs):
        self.logger_header = kargs.get("logger_header", False)
        msg = args[0]
        color_n = msg.count("{}")
        if len(args) != 1 + color_n * 2:
            self._print(
                "log error, \033[1;32m{}\033[0m args expected, \033[1;32m{}\033[0m got, print all args: {}" \
                    .format(1 + color_n * 2, len(args), args), **kargs
            )
            return

        params = list(args[1:1 + color_n])
        for i in args[1 + color_n:]:
            msg = msg.replace(
                "{}", "\033[1;" + self.color_map.get(i) +
                      "m__24FA49F1-7C36-4481-ACF7-BF2146EA4719__\033[0m", 1
            )
        msg = msg.replace("__24FA49F1-7C36-4481-ACF7-BF2146EA4719__", "{}")
        for i in range(len(params)):
            p = str(params[i])
            for j in range(int(p.count("`") / 2)):
                p = p.replace("`", "\033[1;34m", 1)
                p = p.replace("`", "\033[0m", 1)
            params[i] = p
        self._print(msg.format(*params), **kargs)


logger = Logger("tapdata")
# make requests and urllib3 quiet
logging.getLogger("requests").setLevel(get_log_level(config["log.requests_log_level"]))
logging.getLogger("urllib3").setLevel(get_log_level(config["log.urllib3_log_level"]))
