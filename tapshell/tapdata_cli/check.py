import re


class ConfigCheck:
    """
    check job config and set default value
    """

    def __init__(self, config: dict, rules: dict, keep_extra=False):
        """
        param config: job config
        param rules: check rules in tapdata_cli/params/*.py
        param keep_extra: Whether values not in the check rules are saved
        """
        self.config = config
        self.rules = rules
        self.keep_extra = keep_extra
        self.result = {}  # check result
        self.check_order = [  # check sequence
            self._check_require,
            self._check_type,
            self._check_option,
            self._check_reg,
            self._get_child,
        ]

    def check(self):
        for k, v in self.rules.items():
            res = True
            for func in self.check_order:
                res = func(k, v)
                if not res:
                    break
            if res:
                self.result[k] = self.config[k]
                del self.config[k]

    def _get_child(self, k, v):
        """if config value is dict or list"""
        if isinstance(self.config[k], dict) and v.get("value") is not None:
            try:
                self.result[k] = ConfigCheck(self.config[k], v["value"]).checked_config
            except KeyError:
                print(k, v)
                raise Exception
            del self.config[k]
            return False
        elif isinstance(self.config[k], list) and v.get("value") is not None:
            self.result[k] = []
            for i in self.config[k]:
                self.result[k].append(ConfigCheck(i, v["value"]).checked_config)
            del self.config[k]
            return False
        return True

    @property
    def checked_config(self):
        """return the result of check config"""
        self.check()
        if self.keep_extra:
            self.result.update(self.config)
        return self.result

    def _check_require(self, key, value):
        if value.get("require"):
            if not self.config.get(key):
                assert value.get("default") is not None or value.get("value") is not None, \
                    f'{key} is require or must set default value'
                self.config[key] = value.get("default", value.get("type")())
            return True
        else:
            return bool(self.config.get(key))

    def _check_type(self, key, value):
        assert isinstance(self.config.get(key), value.get("type")), f"type of `{key}` must `{value.get('type')}`"
        return True

    def _check_option(self, key, value):
        assert value.get("option") is None or self.config.get(key) in value.get(
            "option"), f"`{key}` must in {value.get('option')}"
        return True

    def _check_reg(self, key, value):
        assert value.get("reg") is None or re.match(value.get("reg"),
                                                    self.config.get(key)) is not None, "Regular expression mismatch"
        return True