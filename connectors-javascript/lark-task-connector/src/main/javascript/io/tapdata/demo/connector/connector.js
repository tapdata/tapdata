config.setStreamReadIntervalSeconds(600);

function discoverSchema(connectionConfig) {
    let app = invoker.invoke("Obtain application information").result;
    let appName = app.data.app.app_name;
    return [{
        'name': appName + '_向群组或用户发送任务',
        'fields': {
            'richSummary': {
                'type': 'String',
                'comment': 'It can be the title of the task. ',
                'nullable': true
            },
            'richDescription': {
                'type': 'String',
                'comment': 'It can be used as a comment for a task. ',
                'nullable': true
            },
            'collaboratorIds': {
                'type': 'String',
                'comment': 'It is used to represent the person responsible for the task, and can be multiple. ',
                'nullable': false
            },
            'time': {
                'type': 'Date',
                'comment': 'It is used to represent the deadline for a task. ',
                'nullable': false
            },
            'followerIds': {
                'type': 'String',
                'comment': 'It is used to represent the followers of a task. ',
                'nullable': true
            },
            'title': {
                'type': 'String',
                'comment': 'The title of the internal description. ',
                'nullable': true
            },
            'url': {
                'type': 'String',
                'comment': 'The title of the internal description can exist as a link, and the url can hold the link address. ',
                'nullable': true
            }
        }
    }];
}

function connectionTest(connectionConfig) {
    let app = invoker.invoke("Obtain application information").result;
    let isApp = 'undefined' !== app && null != app && 'undefined' !== app.data && null != app.data && 'undefined' !== app.data.app;
    let testItem = [{
        "test": "Get App info",
        "code": isApp ? 1 : -1,
        "result": isApp ? "App name is:" + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
    }];
    if (isApp) {
        let needAuthority = {
            "contact:user.id:readonly": '通过手机号或邮箱获取用户 ID',
            "contact:user.employee_id:readonly": '获取用户 user ID',
            "task:task": '查看、创建、编辑和删除任务'
        }
        let appScopes = app.data.app.scopes;
        let hasEnoughAuthority = true;
        let notSupport = '';
        for (let index = 0; index < appScopes.length; index++) {
            let s = appScopes[index];
            let newVar = needAuthority.get(s.scope);
            if ('undefined' === newVar || null == newVar) {
                if (hasEnoughAuthority) {
                    hasEnoughAuthority = false;
                }
                notSupport += s.description + ":" + s.scope + ";";
            }
        }
        testItem.push({
            "test": "Check whether the authority is complete ",
            "code": hasEnoughAuthority ? 1 : -1,
            "result": hasEnoughAuthority ? "Permission configuration meets the requirements" : "The following permissions are configured. Please configure and try again: " + notSupport,
        })
    }
    return testItem;
}

var receiveOpenIdMap = {};

function insertRecord(connectionConfig, nodeConfig, eventDataList) {
    let createTask = new createTask();
    createTask.create(connectionConfig, nodeConfig, eventDataList);
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code != 99991663 && apiResponse.result.code != 99991661) return null;
    let result = invoker.invokeWithoutIntercept("Obtain the App Token and Tenant Token");
    if (result.result.code === 0) return {"token": result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. ');
}

/**
 *
 * @param connectionConfig
 * @param nodeConfig
 * @param commandInfo
 *
 *  "commandInfo": {
 *      "command": String,   //command类型
 *      "action": String,    //action类型
 *      "argMap": Object,    //查询参数
 *      "time": Number       //command发起时间
 *  }
 * */
function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    //
    let commandName = commandInfo.command;
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) {
        return exec.command(connectionConfig, nodeConfig, commandInfo);
    }
}
