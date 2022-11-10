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
    "isUri": {"type": bool, "require": True, "default": False, "option": [False]},
    "database": {"type": str, "require": True},
    "host": {"type": str, "require": True},
    "password": {"type": str, "require": True},
    "ssl": {"type": bool, "require": True, "default": False},
    "user": {"type": str, "require": True},
}


PDK_MYSQL_FORM = {
    "database": {"type": str, "require": True},
    "host": {"type": str, "require": True},
    "username": {"type": str, "require": True},
    "password": {"type": str, "require": True},
    "port": {"type": int, "require": True},
    "additionalString": {"type": str, "require": False},
    "timezone": {"type": str, "require": True, "default": "", "desc": "example: -09:00, +04:00, +00:00"},
}

PDK_POSTGRESQL_FORM = {
    "database": {"type": str, "require": True},
    "host": {"type": str, "require": True},
    "logPluginName": {"type": str, "require": True, "default": "PGOUTPUT", "option": [
        "wal2json", "PGOUTPUT", "wal2json_rds", "wal2json_streaming", "wal2json_rds_streaming", "decoderbufs"
    ]},
    "password": {"type": str, "require": True},
    "port": {"type": int, "require": True},
    "schema": {"type": str, "require": True},
    "user": {"type": str, "require": True},
}

PDK_ORACLE_FORM = {
    "extParams": {"type": str, "require": True, "default": "", "desc": "Connection String Params"},
    "host": {"type": str, "require": True},
    "port": {"type": int, "require": True},
    "logPluginName": {"type": str, "require": True, "default": "logMiner", "option": ["grpc", "logMiner"]},
    # if logPluginName is grpc, the rawLogServerHost/rawLogServerPort must be provided
    "rawLogServerHost": {"type": str, "require": False, "desc": "if logPluginName is grpc, require is True"},
    "rawLogServerPort": {"type": bool, "require": False, "desc": "if rawLogServerPort is grpc, require is True"},

    "schema": {"type": str, "require": True},
    "standBy": {"type": bool, "require": True, "default": False},
    "thinType": {"type": str, "require": True, "default": "SID", "option": ["SID", "SERVICE_NAME"]},
    # SID or SERVER_NAME
    "SERVICE_NAME": {"type": str, "require": False, "desc": "if thinType is SERVICE_NAME, require is True"},
    "sid": {"type": str, "require": False, "desc": "if thinType is SID, require is True"},

    "timezone": {"type": str, "require": True, "default": "", "desc": "example: -09:00, +04:00, +00:00"},
    "user": {"type": str, "require": True},
    "password": {"type": str, "require": True},
    "multiTenant": {"type": bool, "require": False, "default": False},
    # if multiTenant is True, multiTenant is require
    "pdb": {"type": str, "require": False},
}


PDK_KAFKA_FORM = {
    "kafkaAcks": {"type": str, "require": True, "default": "-1"},
    "kafkaCompressionType": {"type": str, "require": True, "default": "gzip", "option": [
        "gzip", "zstd", "lz4", "snappy"
    ]},
    "kafkaIgnoreInvalidRecord": {"type": bool, "require": True, "default": False},
    "kafkaIgnorePushError": {"type": bool, "require": True, "default": False},
    "kafkaSaslMechanism": {"type": str, "require": True, "default": "PLAIN", "option": [
        "PLAIN", "SHA256", "SHA512"
    ]},
    "krb5": {"type": bool, "require": True, "default": False},
    "mqTopicString": {"type": str, "require": False},
    "nameSrvAddr": {"type": str, "require": True},
}


pdk_config = {
    "mongodb": {
        "uri": PDK_MONGO_URI,
        "form": PDK_MONGO_FORM,
    },
    "mysql": {
        "form": PDK_MYSQL_FORM,
    },
    "postgresql": {
        "form": PDK_POSTGRESQL_FORM,
    },
    "oracle": {
        "form": PDK_ORACLE_FORM,
    },
    "kafka": {
        "form": PDK_KAFKA_FORM,
    },
}


