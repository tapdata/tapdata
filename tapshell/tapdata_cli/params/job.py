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
import json
from tapdata_cli.check import ConfigCheck


type = "type"
require = "require"
default = "default"
value = "value"
option = "option"
desc = "desc"
reg = "reg"


MIGRATE = {
    "accessNodeProcessId": {"type": str, "require": True, "default": ""},
    "accessNodeProcessIdList": {"type": list, "require": True, "default": []},
    "accessNodeType": {"type": str, "require": True, "default": "AUTOMATIC_PLATFORM_ALLOCATION"},
    "autoInspect": {"type": str, "require": True, "default": ""},
    "status": {"type": str, "require": True, "default": "paused"},
    "executeMode": {"type": str, "require": True, "default": "normal"},
    "category": {"type": str, "require": True, "default": "task_form_database_clone"},
    "stopOnError": {"type": bool, "require": True, "default": True},
    # "attrs": {"type": dict, "require": True, "default": {}},
    "mappingTemplate": {"type": str, "require": True, "default": "cluster-clone"},
    "dataFlowType": {"type": str, "require": True, "default": "normal"},
    "automaticallyCreateTables": {"type": bool, "require": True, "default": True},
    "isAutoInspect": {"type": bool, "require": True, "default": False},
    "isStopOnError": {"type": bool, "require": True, "default": True},
    "isFilter": {"type": bool, "require": True, "default": False},
    "readShareLogMode": {"type": bool, "require": True, "default": False},
    "isOpenAutoDDL": {"type": bool, "require": True, "default": False},
    "increShareReadMode": {"type": str, "require": True, "default": "STREAMING"},
    "increSyncConcurrency": {"type": bool, "require": True, "default": False},
    "existDataProcessMode": {"type": str, "require": True, "default": "keepData"},
    "writeStrategy": {"type": str, "require": True, "default": "updateOrInsert"},
    "sync_type": {"type": str, "require": True, "default": "initial_sync"},
    "increaseSyncInterval": {"type": int, "require": True, "default": 500},
    "readBatchSize": {"type": int, "require": True, "default": 500},
    "hysteresisInterval": {"type": int, "require": True, "default": 500},
    "increaseReadSize": {"type": int, "require": True, "default": 500},
    "syncType": {"type": str, "require": True, "reg": r"migrate", "default": "migrate"},
    "type": {"type": str, "require": True, "option": ["initial_sync", "cdc", "initial_sync+cdc"], "default": "initial_sync+cdc"},
    "desc": {"type": str, "require": True, "default": "", "desc": "describe a job or pipeline."},
    "planStartDateFlag": {"type": bool, "require": True, "default": False, "desc": "plan start date or not"},
    "planStartDate": {"type": str, "require": False, "desc": "plan start date"},
    "increHysteresis": {"type": bool, "require": True, "default": False},
    "crontabExpression": {"type": str, "require": False,
        "desc": "repeat call task, Example: 0 */1 * * * ? * // Run every minute 0 0 2 * * ? * // Run at 2 every day",
    },
    "isAutoCreateIndex": {"type": bool, "require": True, "default": True},
    "canOpenInspect": {"type": bool, "require": True, "default": True},
    "syncPoints": {"type": list, "require": False, "value": {
        "connectionId": {"require": True, "type": str},
        "connectionName": {"require": True, "type": str},
        "pointType": {"type": str, "require": True, "default": "current"},
        "timeZone": {"type": str, "require": True, "default": "+8"},
    }},
    "shareCdcEnable": {"type": bool, "require": True, "default": False},
    "isSchedule": {"type": bool, "require": True, "default": False},
    "btnDisabled": {"type": dict, "require": True, "value": {
        "delete": {"require": True, "type": bool, "default": False},
        "edit": {"require": True, "type": bool, "default": False},
        "forceStop": {"require": True, "type": bool, "default": True},
        "monitor": {"require": True, "type": bool, "default": True},
        "reset": {"require": True, "type": bool, "default": True},
        "start": {"require": True, "type": bool, "default": False},
        "stop": {"require": True, "type": bool, "default": True},
    }},
    "disabledData": {"type": dict, "require": True, "value": {
        "delete": {"require": True, "type": bool, "default": False},
        "edit": {"require": True, "type": bool, "default": False},
        "forceStop": {"require": True, "type": bool, "default": True},
        "monitor": {"require": True, "type": bool, "default": True},
        "reset": {"require": True, "type": bool, "default": True},
        "start": {"require": True, "type": bool, "default": False},
        "stop": {"require": True, "type": bool, "default": True},
    }},
    "_deleted": {"type": bool, "require": True, "default": False},
}


SYNC = {
    "syncType": {"type": str, "require": True, "reg": r"sync", "default": "sync"},
    "type": {"type": str, "require": True, "option": ["initial_sync", "cdc", "initial_sync+cdc"], "default": "initial_sync+cdc"},
    "desc": {"type": str, "require": True, "default": "", "desc": "describe a job or pipeline."},
    "isAutoCreateIndex": {"type": bool, "require": True, "default": True},
    "deduplicWriteMode": {"type": str, "require": True, "default": "intelligent", "option": ["intelligent", "force"]},
    "increSyncConcurrency": {"type": bool, "require": True, "default": False},
    "increHysteresis": {"type": bool, "require": True, "default": False},
    "hysteresisInterval": {"type": int, "require": False},
    "increaseReadSize": {"type": int, "require": True, "default": 500},
    "increOperationMode": {"type": bool, "require": True, "default": False},
    "processorThreadNum": {"type": int, "require": True, "default": 8},
    "shareCdcEnable": {"type": bool, "require": True, "default": False},
    "syncPoints": {"type": list, "require": False, "value": {
        "connectionId": {"require": True, "type": str},
        "connectionName": {"require": True, "type": str},
        "pointType": {"type": str, "require": True, "default": "current"},
        "timeZone": {"type": str, "require": True, "default": "+8"},
    }},
    "crontabExpression": {"type": str, "require": False,
        "desc": "repeat call task, Example: 0 */1 * * * ? * // Run every minute 0 0 2 * * ? * // Run at 2 every day",
    },
}


NODE_SOURCE = {
    "attrs": {type: dict, require: True, value: {
        "accessNodeProcessId": {require: True, default: "", type: str},
        "capabilities": {require: False, type: list},
        "connectionName": {require: True, type: str},
        "connectionType": {require: True, type: str, option: ["source_and_target", "source", "target"]},
        "pdkHash": {require: True, type: str},
        "pdkType": {require: True, type: str},
        "position": {require: True, type: list},
    }},
    "connectionId": {require: True, type: str},
    # TODO: check enable database
    "databaseType": {require: True, type: str},
    "disabledEvents": {require: True, type: list, default: [], value: {
        "new_field_event": {require: False, type: str},
        "alter_field_name_event": {require: False, type: str},
        "alter_field_attributes_event": {require: False, type: str},
        "drop_field_event": {require: False, type: str},

    }},
    "enableDDL": {require: False, type: bool, default: False},
    "id": {require: True, type: str},
    "increaseReadSize": {require: True, type: int, default: 100},
    "migrateTableSelectType": {require: True, type: str, default: "all", option: ["all", "custom"]},
    "name": {require: True, type: str},
    "enableDynamicTable": {require: False, type: bool, default: False},
    "tableNames": {require: False, type: list},
}


NODE_TARGET = {
    "attrs": {type: dict, require: True, value: {
        "accessNodeProcessId": {require: True, default: "", type: str},
        "capabilities": {require: False, type: list},
        "connectionName": {require: True, type: str},
        "connectionType": {require: True, type: str, option: ["source_and_target", "source", "target"]},
        "pdkHash": {require: True, type: str},
        "pdkType": {require: True, type: str},
        "position": {require: True, type: list},
    }},
    "connectionId": {require: True, type: str},
    "databaseType": {require: True, type: str},
    "id": {require: True, type: str},
    "migrateTableSelectType": {require: True, type: str, default: "all", option: ["all", "custom"]},
    "name": {require: True, type: str},
    "dmlPolicy": {require: False, type: dict, value: {
        "insertPolicy": {require: False, type: str, option: ["update_on_exists", "ignore_on_exists"]},
        "updatePolicy": {require: False, type: str, option: ["ignore_on_nonexists", "insert_on_nonexists"]},
    }},
    "initialConcurrent": {require: True, type: bool, default: False},
    "initialConcurrentWriteNum": {require: True, type: int, default: 8},
    "isTransformed": {require: True, type: bool, default: False},
    "cdcConcurrent": {require: True, type: bool, default: False},
    "cdcConcurrentWriteNum": {require: True, type: int, default: 4},
    "existDataProcessMode": {require: True, type: str, default: "keepData", option: ["dropTable", "removeData", "keepData"]},
    "isFilter": {require: False, type: bool, default: False},
}


NODE_SOURCE_SYNC = {
    "attrs": {type: dict, require: True, value: {
        "accessNodeProcessId": {require: True, default: "", type: str},
        "capabilities": {require: False, type: list},
        "connectionName": {require: True, type: str},
        "connectionType": {require: True, type: str, option: ["source_and_target", "source", "target"]},
        "pdkHash": {require: True, type: str},
        "pdkType": {require: True, type: str},
        "position": {require: True, type: list},
    }},
    "connectionId": {require: True, type: str},
    # TODO: check enable database
    "databaseType": {require: True, type: str},
    "disabledEvents": {require: True, type: list, default: [], value: {
        "new_field_event": {require: False, type: str},
        "alter_field_name_event": {require: False, type: str},
        "alter_field_attributes_event": {require: False, type: str},
        "drop_field_event": {require: False, type: str},
    }},
    "enableDDL": {require: False, type: bool, default: False},
    "id": {require: True, type: str},
    "increaseReadSize": {require: True, type: int, default: 100},
    "increaseSyncInterval": {"type": int, "require": True, "default": 500},
    "name": {require: True, type: str},
    "tableName": {require: True, type: str},
    "type": {require: True, type: str, default: "table"},
    "node_config": {require: True, type: dict, default: {}},
    "isFilter": {require: False, type: bool, default: False},
}


NODE_TARGET_SYNC = {
    "node_config": {require: True, type: dict, default: {}},
    "attrs": {type: dict, require: True, value: {
        "accessNodeProcessId": {require: True, default: "", type: str},
        "capabilities": {require: False, type: list},
        "connectionName": {require: True, type: str},
        "connectionType": {require: True, type: str, option: ["source_and_target", "source", "target"]},
        "pdkHash": {require: True, type: str},
        "pdkType": {require: True, type: str},
        "position": {require: True, type: list},
    }},
    "connectionId": {require: True, type: str},
    "databaseType": {require: True, type: str},
    "id": {require: True, type: str},
    "name": {require: True, type: str},
    "writeStrategy": {require: True, type: str, option: ["appendWrite", "updateOrInsert"], default: "updateOrInsert"},
    "initialConcurrent": {require: True, type: bool, default: False},
    "initialConcurrentWriteNum": {require: True, type: int, default: 8},
    "cdcConcurrent": {require: True, type: bool, default: False},
    "cdcConcurrentWriteNum": {require: True, type: int, default: 4},
    "tableName": {require: True, type: str},
    "type": {require: True, type: str, default: "table"},
    "updateConditionFields": {require: True, type: list, default: ["_id"]},
    "existDataProcessMode": {require: True, type: str, default: "keepData", option: ["dropTable", "removeData", "keepData"]},
}


job_config = {
    "migrate": MIGRATE,
    "sync": SYNC,
}

node_config = {
    "source": NODE_SOURCE,
    "sink": NODE_TARGET,
}


node_config_sync = {
    "source": NODE_SOURCE_SYNC,
    "sink": NODE_TARGET_SYNC,
}


if __name__ == '__main__':
    config = {
        "type": "initial_sync",
    }
    resp = ConfigCheck(config, MIGRATE).checked_config
    print(json.dumps(resp, indent=4))
