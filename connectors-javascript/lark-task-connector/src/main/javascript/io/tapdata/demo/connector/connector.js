function discoverSchema(connectionConfig) {
    let app = invoker.invoke("Obtain application information").result;
    let appName = app.data.app.app_name;
    return [{
        'name': appName + '_send_tasks',
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
                'type': 'String',
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
        "result": isApp ? "App name is: " + app.data.app.app_name : "Can not get App info, please check you App ID and App Secret."
    }];
    return testItem;
}

var createTask = new CreateTask();

function insertRecord(connectionConfig, nodeConfig, eventDataMap) {
    return createTask.create(connectionConfig, nodeConfig, eventDataMap);
}

function updateToken(connectionConfig, nodeConfig, apiResponse) {
    if (apiResponse.result.code !== 99991663 && apiResponse.result.code !== 99991661) return null;
    let result = invoker.invokeWithoutIntercept("Obtain the App Token and Tenant Token");
    if (result.result.code === 0) return {"Authorization": "Bearer " + result.result.tenant_access_token};
    else log.error('Cannot get tenant access token, please check your app_id or app_secret or check api named GetAppToken. {}', result.result);
}

function commandCallback(connectionConfig, nodeConfig, commandInfo) {
    let commandName = commandInfo.command;
    let exec = new CommandStage().exec(commandInfo.command);
    if (null != exec) return exec.command(connectionConfig, nodeConfig, commandInfo);
}
