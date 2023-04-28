function discoverSchema(connectionConfig) {
    // TODO
    let codeArr = [];
    codeArr.push(connectionConfig.definition_code);
    // let nameArr = connectionConfig.conditions[value];
    let res = []
    for (let index = 0; index < codeArr.length; index++) {
        res[index] = {
            // TODO
            "name": codeArr[index],
            "fields": {
                'instance_code': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false,
                    'isPrimaryKey': true,
                    'primaryKeyPos': 1
                },
                'approval_code': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'approval_name': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'department_id': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'end_time': {
                    'type': 'Number',
                    'comment': '',
                    'nullable': false
                },
                'form': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'open_id': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'reverted': {
                    'type': 'Boolean',
                    'comment': '',
                    'nullable': true
                },
                'serial_number': {
                    'type': 'Number',
                    'comment': '',
                    'nullable': false
                },
                'start_time': {
                    'type': 'Number',
                    'comment': '',
                    'nullable': false
                },
                'status': {
                    'type': 'Boolean',
                    'comment': '',
                    'nullable': false
                },
                'task_list': {
                    'type': 'Array',
                    'comment': '',
                    'nullable': false
                },
                'timeline': {
                    'type': 'Array',
                    'comment': '',
                    'nullable': false
                },
                'user_id': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                },
                'uuid': {
                    'type': 'String',
                    'comment': '',
                    'nullable': false
                }
            }
        }
    }
    return res;
}

var clientInfo = {
    "start_time": "1555344000000"
}

function timestampToOffset(time) {
    return {
        "lastModifiedDate": time,
        "firstTime": true,
        "currentTime": time,
        "page_token": null
    }
}

function batchRead(connectionConfig, nodeConfig, offset, tableName, pageSize, batchReadSender) {
    if (null == offset || "undefined" === offset.firstTime || null == offset.firstTime) {
        offset = timestampToOffset(offset);
    }
    //TODO
    // clientInfo.approval_code = tableName.split("_")[1];
    clientInfo.approval_code = tableName;
    clientInfo.end_time = "" + Number(new Date(new Date().getTime()));
    let isFirst = false;
    let instancesList;
    if (offset && null == offset.page_token) {
        if (offset.firstTime) {
            isFirst = true;
            offset.firstTime = false;
            clientInfo.page_token = "";
        } else {
            clientInfo.page_token = "page_token=" + offset.page_token + "&";
        }
    } else {
        throw "Unexpected offset " + JSON.stringify(offset);
    }
    do {
        try {
            if (isFirst) {
                instancesList = invoker.invoke("getInstancesList", clientInfo).result;
                isFirst = false
            } else {
                clientInfo.page_token = "page_token=" + offset.page_token + "&";
                instancesList = invoker.invoke("getInstancesList", clientInfo).result;
            }
        } catch (e) {
            throw ("Failed to query the data. Please check the connection." + JSON.stringify(instancesList))
        }
        if (instancesList && instancesList.data && instancesList.data.has_more) {
            if (!(instancesList && instancesList.data && instancesList.data.instance_code_list && instancesList.data.page_token)) {
                throw ("Failed to read the instancesList event. Please check the network or whether you have permission.")
            }
        }
        offset.page_token = instancesList.data.page_token;
        let instance_code_list = instancesList.data.instance_code_list;
        let result = [];
        for (let index = 0; index < instance_code_list.length; index++) {
            let instance;
            try {
                clientInfo.instance_id = instance_code_list[index];
                instance = invoker.invoke("getInstances", clientInfo).result
            } catch (e) {
                throw ("Failed to query the data. Please check the connection." + JSON.stringify(instance))
            }
            if (!(instance && instance.data && instance.data.instance_code && instance.data.user_id)) {
                log.warn("Failed to read a single approval event. Check the network or whether you have permission.")
                continue;
            }
            result.push(instance.data);
        }
        batchReadSender.send(result, tableName, offset)
        result = [];
    } while (instancesList.data.has_more);
}

function connectionTest(connectionConfig) {
    log.warn("connectionconfig:{}", connectionConfig)
    let instancesList;
    let httpCode;
    let res = [];
    let app;
    try {
        // TODO
        clientInfo.approval_code = connectionConfig.definition_code;
        // TODO
        clientInfo.end_time = "" + Number(new Date(new Date().getTime()));
        clientInfo.page_token = "";
        app = invoker.invoke("getToken").result;
        res.push({
            "test": "Get App Token",
            "code": appCode(app),
            "result": getTokenResult(app)
        })
        instancesList = invoker.invoke("getInstancesList", clientInfo).result;
        log.warn("clientInfo1a:{} e:{} s:{}", clientInfo.approval_code, clientInfo.end_time, clientInfo.start_time)
        res.push({
            "test": "Permission check",
            "code": instancesListCode(instancesList),
            "result": result(instancesList)
        })
        return res;
    } catch (e) {
        return [
            {
                "test": "Authorization failed",
                "code": -1,
                "result": exceptionUtil.eMessage(e)
            }
        ];
    }
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661) return null;
    let result = invoker.invokeWithoutIntercept("getToken");
    if (result.result.code === 0) return {"Authorization": "Bearer " + result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. {}', result.result);
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    let app;
    log.warn("commandinfo:{}",JSON.stringify(commandInfo))
    if (commandInfo.command.substring(0, 7) === "getToken") {
        log.warn("get token1:{}")
        return {"setValue": {
            "definitionCode": {
                "data": commandInfo.command.split('getToken')[0]
            }
        }};
        //return "假期申请";
        log.warn("get token1:{}")
        try {
            app = invoker.invoke("getToken").result;
            if (!(app && app.app_access_token)) {
                throw ("Can not get App info, please check you App ID and App Secret.")
            }
        } catch (e) {
            throw ("Failed to query the data. Please check the connection." + JSON.stringify(app))
        }
        let token = app.app_access_token;
        let instancesList;
        let instances;
        try {
            clientInfo.end_time = Number(new Date(new Date().getTime())) + "";
            clientInfo.Authorization = token;
            instancesList = invoker.invoke("getInstancesList", clientInfo).result;
            if (!(instancesList && instancesList.data) || instancesList.data.instance_code_list.length === 0) {
                throw ("There is no record of this approval type.")
            }

            clientInfo.instance_id = instancesList.data.instance_code_list[0];
            instances = invoker.invoke("getInstances", clientInfo).result;
            if (!(instances && instances.data && instances.data.approval_name)) {
                throw ("Query failure")
            }
            return instances.data.approval_name;
        } catch (e) {
            throw ("Failed to query the data. Please check the connection.")
        }
    }

    let commandName = commandInfo.command;
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) return exec.command(connectionConfig, nodeConfig, commandInfo);
}
