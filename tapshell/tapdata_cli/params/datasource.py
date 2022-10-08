"""
the file containers many check rules
rules interpretation:
    type    - value type
    require - whether the value must exist
    reg     - Regular expression string
    option  - optional values
    desc    - describe the value
    default - default value
"""


DATASOURCE_CONFIG = {
    "accessNodeType": {
        "type": str, "default": "AUTOMATIC_PLATFORM_ALLOCATION", "require": True,
        "option": ["MANUALLY_SPECIFIED_BY_THE_USER", "AUTOMATIC_PLATFORM_ALLOCATION"],
        "desc": "agent settings, use manual or automatic"
    },
    "accessNodeProcessId": {"type": str, "require": False},
    "connection_type": {
        "type": str, "default": "source_and_target", "require": True,
        "option": ["source", "target", "source_and_target"],
        "desc": "This data connection can be used as source and target at the same time",
    },
    "database_type": {"type": str, "require": True},
    "loadAllTables": {"type": bool, "default": True, "require": True},
    "name": {"type": str, "require": True},
    "pdkHash": {"type": str, "require": True},
    "pdkType": {"type": str, "require": True, "default": "pdk"},
    "project": {"type": str, "require": True, "default": ""},
    "response_body": {"type": dict, "require": True, "default": {}},
    "retry": {"type": int, "require": True, "default": 0},
    "schema": {"type": dict, "require": True, "default": {}},
    "shareCdcEnable": {"type": bool, "require": True, "default": True,
                       "desc": "Shared mining will mine incremental logs"},
    "status": {"type": str, "require": True, "default": "testing"},
    "submit": {"type": bool, "require": True, "default": True},
    "table_filter": {"type": str, "require": False},
}


PDK_MONGO_URI = {
    "isUri": {"type": bool, "require": True, "default": True, "option": [True]},
    "ssl": {"type": bool, "require": True, "default": False},
    "uri": {"type": str, "require": True},
}


PDK_MONGO_FORM = {
    "additionalString": {"type": str, "require": False},
    "database": {"type": str, "require": True},
    "host": {"type": str, "require": True},
    "isUri": {"type": bool, "require": False, "default": False, "option": [False]},
    "password": {"type": str, "require": True},
    "ssl": {"type": bool, "require": True, "default": False},
    "user": {"type": str, "require": True},
}


pdk_config = {
    "mongodb": {
        "uri": PDK_MONGO_URI,
        "form": PDK_MONGO_FORM,
    },
}


